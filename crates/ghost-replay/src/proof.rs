//! Replay proof system for formal verification of runtime correctness.
//!
//! Provides a serializable `ReplayProof` that certifies the equivalence (or
//! divergence) of two replay runs across domains.  Proofs can be generated
//! with [`prove_replay_equivalence`] and independently verified with
//! [`verify_replay_proof`].

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

use ghost_core::trace::{current_timestamp, TraceEvent};

use crate::checksum::{from_events, ReplayChecksum};
use crate::format::Domain;
use crate::state_reconstructor::StateReconstructor;

// ─── Proof Types ──────────────────────────────────────────────────────────────

/// A formal proof that two replay runs are equivalent (or divergent).
///
/// Serializable to JSON for independent verification and audit trails.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayProof {
    /// Proof format version.
    pub version: u32,
    /// Human-readable label for this proof.
    pub label: String,
    /// The baseline domain.
    pub baseline_domain: ProofDomain,
    /// The candidate domain.
    pub candidate_domain: ProofDomain,
    /// Number of events in the baseline stream.
    pub baseline_event_count: usize,
    /// Number of events in the candidate stream.
    pub candidate_event_count: usize,
    /// Blake3 checksum of the baseline event stream.
    pub baseline_checksum: String,
    /// Blake3 checksum of the candidate event stream.
    pub candidate_checksum: String,
    /// Whether the two streams are equivalent.
    pub equivalent: bool,
    /// Classification of the proof outcome.
    pub outcome: ProofOutcome,
    /// State snapshots at points of divergence (if any).
    pub divergence_points: Vec<ProofDivergencePoint>,
    /// Timestamp when the proof was generated (unix millis).
    pub generated_at: u64,
    /// SHA-256 hash of the proof content (excluding this field itself).
    pub proof_hash: String,
}

/// Domain identifier used inside proofs (serializable version of [`Domain`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProofDomain {
    /// CPU simulation domain.
    CpuSimulation,
    /// Disk I/O domain.
    DiskIo,
    /// Failure injection domain.
    FailureInjected,
    /// Deterministic replay domain.
    Deterministic,
    /// Real I/O domain.
    RealIo,
}

impl From<Domain> for ProofDomain {
    fn from(d: Domain) -> Self {
        match d {
            Domain::CpuSimulation => ProofDomain::CpuSimulation,
            Domain::DiskIo => ProofDomain::DiskIo,
            Domain::FailureInjected => ProofDomain::FailureInjected,
            Domain::Deterministic => ProofDomain::Deterministic,
            Domain::RealIo => ProofDomain::RealIo,
        }
    }
}

impl fmt::Display for ProofDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProofDomain::CpuSimulation => write!(f, "cpu-simulation"),
            ProofDomain::DiskIo => write!(f, "disk-io"),
            ProofDomain::FailureInjected => write!(f, "failure-injected"),
            ProofDomain::Deterministic => write!(f, "deterministic"),
            ProofDomain::RealIo => write!(f, "real-io"),
        }
    }
}

/// Classification of a proof outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProofOutcome {
    /// The two streams are fully equivalent (same decisions, same timing).
    Identical,
    /// The two streams made the same decisions but with different timing.
    TimingOnly,
    /// The two streams made at least one different decision.
    Divergent,
    /// The two streams had different lengths.
    LengthMismatch,
}

impl fmt::Display for ProofOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProofOutcome::Identical => write!(f, "IDENTICAL"),
            ProofOutcome::TimingOnly => write!(f, "TIMING_ONLY"),
            ProofOutcome::Divergent => write!(f, "DIVERGENT"),
            ProofOutcome::LengthMismatch => write!(f, "LENGTH_MISMATCH"),
        }
    }
}

/// A single point of divergence between two replay streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofDivergencePoint {
    /// Event index where divergence was detected.
    pub event_index: usize,
    /// Whether this is a timing-only difference.
    pub timing_only: bool,
    /// Baseline state summary at this point.
    pub baseline_summary: Option<String>,
    /// Candidate state summary at this point.
    pub candidate_summary: Option<String>,
}

impl ProofDivergencePoint {
    /// Creates a timing-only divergence point.
    pub fn timing(event_index: usize) -> Self {
        Self {
            event_index,
            timing_only: true,
            baseline_summary: None,
            candidate_summary: None,
        }
    }

    /// Creates a decision divergence point with state summaries.
    pub fn decision(
        event_index: usize,
        baseline_summary: String,
        candidate_summary: String,
    ) -> Self {
        Self {
            event_index,
            timing_only: false,
            baseline_summary: Some(baseline_summary),
            candidate_summary: Some(candidate_summary),
        }
    }
}

// ─── Proof Generation ─────────────────────────────────────────────────────────

/// Generates a replay proof by comparing two event streams across domains.
///
/// This is the primary entry point for creating a formal proof of
/// equivalence (or divergence) between two replay runs.
pub fn prove_replay_equivalence(
    label: &str,
    baseline_events: &[TraceEvent],
    candidate_events: &[TraceEvent],
    baseline_domain: Domain,
    candidate_domain: Domain,
) -> ReplayProof {
    let baseline_checksum = from_events(baseline_events);
    let candidate_checksum = from_events(candidate_events);

    let baseline_event_count = baseline_events.len();
    let candidate_event_count = candidate_events.len();

    // Check length mismatch first
    if baseline_event_count != candidate_event_count {
        return build_proof(
            label,
            baseline_domain,
            candidate_domain,
            baseline_event_count,
            candidate_event_count,
            &baseline_checksum,
            &candidate_checksum,
            false,
            ProofOutcome::LengthMismatch,
            Vec::new(),
        );
    }

    // Run state reconstruction on both streams
    let mut baseline_recon = StateReconstructor::new();
    let mut candidate_recon = StateReconstructor::new();
    baseline_recon.process_events(baseline_events);
    candidate_recon.process_events(candidate_events);

    let diffs = StateReconstructor::compare(baseline_recon.snapshots(), candidate_recon.snapshots());

    if diffs.is_empty() {
        // Fully identical
        return build_proof(
            label,
            baseline_domain,
            candidate_domain,
            baseline_event_count,
            candidate_event_count,
            &baseline_checksum,
            &candidate_checksum,
            true,
            ProofOutcome::Identical,
            Vec::new(),
        );
    }

    // Classify divergences
    let mut _timing_count = 0usize;
    let mut decision_count = 0usize;
    let mut divergence_points = Vec::new();

    for &idx in &diffs {
        let baseline_snap = baseline_recon.snapshot_at(idx);
        let candidate_snap = candidate_recon.snapshot_at(idx);

        if let (Some(bs), Some(cs)) = (baseline_snap, candidate_snap) {
            if bs.chunk_states == cs.chunk_states
                && bs.total_allocated == cs.total_allocated
                && bs.total_stored == cs.total_stored
            {
                _timing_count += 1;
                divergence_points.push(ProofDivergencePoint::timing(idx));
            } else {
                decision_count += 1;
                divergence_points.push(ProofDivergencePoint::decision(
                    idx,
                    format!("chunks={} alloc={} stored={}", bs.chunk_states.len(), bs.total_allocated, bs.total_stored),
                    format!("chunks={} alloc={} stored={}", cs.chunk_states.len(), cs.total_allocated, cs.total_stored),
                ));
            }
        } else {
            decision_count += 1;
            divergence_points.push(ProofDivergencePoint::decision(
                idx,
                "snapshot unavailable".to_string(),
                "snapshot unavailable".to_string(),
            ));
        }
    }

    let outcome = if decision_count > 0 {
        ProofOutcome::Divergent
    } else {
        ProofOutcome::TimingOnly
    };

    let equivalent = decision_count == 0;

    build_proof(
        label,
        baseline_domain,
        candidate_domain,
        baseline_event_count,
        candidate_event_count,
        &baseline_checksum,
        &candidate_checksum,
        equivalent,
        outcome,
        divergence_points,
    )
}

/// Generates a replay proof from trace files.
pub fn prove_replay_from_files(
    label: &str,
    baseline_path: &Path,
    candidate_path: &Path,
    baseline_domain: Domain,
    candidate_domain: Domain,
) -> ghost_core::error::GhostResult<ReplayProof> {
    let mut baseline_reader = crate::reader::TraceReader::open(baseline_path)?;
    let baseline_events = baseline_reader.read_all()?;

    let mut candidate_reader = crate::reader::TraceReader::open(candidate_path)?;
    let candidate_events = candidate_reader.read_all()?;

    Ok(prove_replay_equivalence(
        label,
        &baseline_events,
        &candidate_events,
        baseline_domain,
        candidate_domain,
    ))
}

// ─── Proof Verification ───────────────────────────────────────────────────────

/// Verifies a replay proof by re-computing checksums and comparing outcomes.
///
/// Returns `Ok(())` if the proof is valid, or an error describing the
/// verification failure.
pub fn verify_replay_proof(
    proof: &ReplayProof,
    baseline_events: &[TraceEvent],
    candidate_events: &[TraceEvent],
) -> Result<(), ProofVerificationError> {
    // Recompute checksums
    let baseline_checksum = from_events(baseline_events);
    let candidate_checksum = from_events(candidate_events);

    // Verify baseline checksum
    if proof.baseline_checksum != baseline_checksum.to_string() {
        return Err(ProofVerificationError::BaselineChecksumMismatch {
            expected: proof.baseline_checksum.clone(),
            actual: baseline_checksum.to_string(),
        });
    }

    // Verify candidate checksum
    if proof.candidate_checksum != candidate_checksum.to_string() {
        return Err(ProofVerificationError::CandidateChecksumMismatch {
            expected: proof.candidate_checksum.clone(),
            actual: candidate_checksum.to_string(),
        });
    }

    // Verify event counts
    if proof.baseline_event_count != baseline_events.len() {
        return Err(ProofVerificationError::EventCountMismatch {
            domain: "baseline".to_string(),
            expected: proof.baseline_event_count,
            actual: baseline_events.len(),
        });
    }
    if proof.candidate_event_count != candidate_events.len() {
        return Err(ProofVerificationError::EventCountMismatch {
            domain: "candidate".to_string(),
            expected: proof.candidate_event_count,
            actual: candidate_events.len(),
        });
    }

    // Verify equivalence outcome
    let mut baseline_recon = StateReconstructor::new();
    let mut candidate_recon = StateReconstructor::new();
    baseline_recon.process_events(baseline_events);
    candidate_recon.process_events(candidate_events);

    let diffs = StateReconstructor::compare(baseline_recon.snapshots(), candidate_recon.snapshots());

    let actual_equivalent = diffs.is_empty() || diffs.iter().all(|&idx| {
        let bs = baseline_recon.snapshot_at(idx);
        let cs = candidate_recon.snapshot_at(idx);
        match (bs, cs) {
            (Some(bs), Some(cs)) => {
                bs.chunk_states == cs.chunk_states
                    && bs.total_allocated == cs.total_allocated
                    && bs.total_stored == cs.total_stored
            }
            _ => false,
        }
    });

    if proof.equivalent != actual_equivalent {
        return Err(ProofVerificationError::OutcomeMismatch {
            expected_equivalent: proof.equivalent,
            actual_equivalent,
        });
    }

    Ok(())
}

/// Errors that can occur during proof verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProofVerificationError {
    /// Baseline checksum does not match.
    BaselineChecksumMismatch {
        expected: String,
        actual: String,
    },
    /// Candidate checksum does not match.
    CandidateChecksumMismatch {
        expected: String,
        actual: String,
    },
    /// Event count does not match.
    EventCountMismatch {
        domain: String,
        expected: usize,
        actual: usize,
    },
    /// Equivalence outcome does not match.
    OutcomeMismatch {
        expected_equivalent: bool,
        actual_equivalent: bool,
    },
}

impl fmt::Display for ProofVerificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BaselineChecksumMismatch { expected, actual } => {
                write!(f, "Baseline checksum mismatch: expected {}, got {}", expected, actual)
            }
            Self::CandidateChecksumMismatch { expected, actual } => {
                write!(f, "Candidate checksum mismatch: expected {}, got {}", expected, actual)
            }
            Self::EventCountMismatch { domain, expected, actual } => {
                write!(f, "Event count mismatch for {}: expected {}, got {}", domain, expected, actual)
            }
            Self::OutcomeMismatch { expected_equivalent, actual_equivalent } => {
                write!(
                    f,
                    "Outcome mismatch: proof claims equivalent={}, actual={}",
                    expected_equivalent, actual_equivalent
                )
            }
        }
    }
}

impl std::error::Error for ProofVerificationError {}

// ─── Serialization ────────────────────────────────────────────────────────────

/// Serializes a proof to JSON.
pub fn proof_to_json(proof: &ReplayProof) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(proof)
}

/// Deserializes a proof from JSON.
pub fn proof_from_json(json: &str) -> Result<ReplayProof, serde_json::Error> {
    serde_json::from_str(json)
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

fn build_proof(
    label: &str,
    baseline_domain: Domain,
    candidate_domain: Domain,
    baseline_event_count: usize,
    candidate_event_count: usize,
    baseline_checksum: &ReplayChecksum,
    candidate_checksum: &ReplayChecksum,
    equivalent: bool,
    outcome: ProofOutcome,
    divergence_points: Vec<ProofDivergencePoint>,
) -> ReplayProof {
    let generated_at = current_timestamp();

    // Build a proof without hash first, then compute hash
    let mut proof = ReplayProof {
        version: 1,
        label: label.to_string(),
        baseline_domain: baseline_domain.into(),
        candidate_domain: candidate_domain.into(),
        baseline_event_count,
        candidate_event_count,
        baseline_checksum: baseline_checksum.to_string(),
        candidate_checksum: candidate_checksum.to_string(),
        equivalent,
        outcome,
        divergence_points,
        generated_at,
        proof_hash: String::new(),
    };

    // Compute proof hash
    proof.proof_hash = compute_proof_hash(&proof);
    proof
}

fn compute_proof_hash(proof: &ReplayProof) -> String {
    // Create a copy with empty hash, serialize, and hash
    let mut copy = proof.clone();
    copy.proof_hash = String::new();

    if let Ok(json) = serde_json::to_string(&copy) {
        let hash = blake3::hash(json.as_bytes());
        hash.to_hex().to_string()
    } else {
        String::from("hash-error")
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;
    use ghost_core::types::{ChunkId, TierId};

    fn sample_events() -> Vec<TraceEvent> {
        vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"proof-test"),
                timestamp: 5000,
                size: 512,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"proof-test"),
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
                timestamp: 5001,
            },
            TraceEvent::ChunkDeleted {
                chunk_id: ChunkId::from_data(b"proof-test"),
                timestamp: 5010,
                tier: TierId::Ram,
            },
        ]
    }

    #[test]
    fn test_prove_identical_streams() {
        let events = sample_events();
        let proof = prove_replay_equivalence(
            "test-identical",
            &events,
            &events,
            Domain::CpuSimulation,
            Domain::DiskIo,
        );

        assert!(proof.equivalent);
        assert_eq!(proof.outcome, ProofOutcome::Identical);
        assert!(proof.divergence_points.is_empty());
        assert_eq!(proof.baseline_event_count, 3);
        assert_eq!(proof.candidate_event_count, 3);
    }

    #[test]
    fn test_prove_divergent_streams() {
        let baseline = sample_events();
        let mut candidate = sample_events();
        // Change a decision in the candidate
        candidate[1] = TraceEvent::ChunkStateChanged {
            chunk_id: ChunkId::from_data(b"proof-test"),
            from: ChunkState::Allocated,
            to: ChunkState::Cached, // different from Stored
            timestamp: 5001,
        };

        let proof = prove_replay_equivalence(
            "test-divergent",
            &baseline,
            &candidate,
            Domain::CpuSimulation,
            Domain::FailureInjected,
        );

        assert!(!proof.equivalent);
        assert_eq!(proof.outcome, ProofOutcome::Divergent);
        assert!(!proof.divergence_points.is_empty());
    }

    #[test]
    fn test_prove_length_mismatch() {
        let baseline = sample_events();
        let candidate = vec![baseline[0].clone()]; // shorter

        let proof = prove_replay_equivalence(
            "test-length",
            &baseline,
            &candidate,
            Domain::Deterministic,
            Domain::RealIo,
        );

        assert!(!proof.equivalent);
        assert_eq!(proof.outcome, ProofOutcome::LengthMismatch);
    }

    #[test]
    fn test_verify_proof_valid() {
        let events = sample_events();
        let proof = prove_replay_equivalence(
            "test-verify",
            &events,
            &events,
            Domain::CpuSimulation,
            Domain::DiskIo,
        );

        let result = verify_replay_proof(&proof, &events, &events);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_proof_invalid_checksum() {
        let events = sample_events();
        let mut proof = prove_replay_equivalence(
            "test-invalid",
            &events,
            &events,
            Domain::CpuSimulation,
            Domain::DiskIo,
        );

        // Tamper with the checksum
        proof.baseline_checksum = "tampered".to_string();

        let result = verify_replay_proof(&proof, &events, &events);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ProofVerificationError::BaselineChecksumMismatch {
                expected: "tampered".to_string(),
                actual: from_events(&events).to_string(),
            }
        );
    }

    #[test]
    fn test_proof_json_roundtrip() {
        let events = sample_events();
        let proof = prove_replay_equivalence(
            "test-json",
            &events,
            &events,
            Domain::Deterministic,
            Domain::RealIo,
        );

        let json = proof_to_json(&proof).unwrap();
        let deserialized = proof_from_json(&json).unwrap();

        assert_eq!(proof.version, deserialized.version);
        assert_eq!(proof.label, deserialized.label);
        assert_eq!(proof.equivalent, deserialized.equivalent);
        assert_eq!(proof.outcome, deserialized.outcome);
        assert_eq!(proof.proof_hash, deserialized.proof_hash);
    }

    #[test]
    fn test_proof_hash_nonempty() {
        let events = sample_events();
        let proof = prove_replay_equivalence(
            "test-hash",
            &events,
            &events,
            Domain::CpuSimulation,
            Domain::DiskIo,
        );

        assert!(!proof.proof_hash.is_empty());
        assert_ne!(proof.proof_hash, "hash-error");
    }

    #[test]
    fn test_proof_domain_display() {
        assert_eq!(format!("{}", ProofDomain::CpuSimulation), "cpu-simulation");
        assert_eq!(format!("{}", ProofDomain::DiskIo), "disk-io");
        assert_eq!(format!("{}", ProofDomain::FailureInjected), "failure-injected");
        assert_eq!(format!("{}", ProofDomain::Deterministic), "deterministic");
        assert_eq!(format!("{}", ProofDomain::RealIo), "real-io");
    }

    #[test]
    fn test_proof_outcome_display() {
        assert_eq!(format!("{}", ProofOutcome::Identical), "IDENTICAL");
        assert_eq!(format!("{}", ProofOutcome::TimingOnly), "TIMING_ONLY");
        assert_eq!(format!("{}", ProofOutcome::Divergent), "DIVERGENT");
        assert_eq!(format!("{}", ProofOutcome::LengthMismatch), "LENGTH_MISMATCH");
    }

    #[test]
    fn test_divergence_point_timing() {
        let dp = ProofDivergencePoint::timing(42);
        assert_eq!(dp.event_index, 42);
        assert!(dp.timing_only);
        assert!(dp.baseline_summary.is_none());
    }

    #[test]
    fn test_divergence_point_decision() {
        let dp = ProofDivergencePoint::decision(7, "baseline_state".into(), "candidate_state".into());
        assert_eq!(dp.event_index, 7);
        assert!(!dp.timing_only);
        assert_eq!(dp.baseline_summary, Some("baseline_state".to_string()));
        assert_eq!(dp.candidate_summary, Some("candidate_state".to_string()));
    }

    #[test]
    fn test_proof_from_domain_conversion() {
        assert_eq!(ProofDomain::from(Domain::CpuSimulation), ProofDomain::CpuSimulation);
        assert_eq!(ProofDomain::from(Domain::DiskIo), ProofDomain::DiskIo);
        assert_eq!(ProofDomain::from(Domain::FailureInjected), ProofDomain::FailureInjected);
        assert_eq!(ProofDomain::from(Domain::Deterministic), ProofDomain::Deterministic);
        assert_eq!(ProofDomain::from(Domain::RealIo), ProofDomain::RealIo);
    }
}
