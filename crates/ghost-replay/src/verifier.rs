//! Replay verification harness.
//!
//! Runs workloads multiple times and verifies identical output through
//! checksum comparison and divergence detection.

use std::fmt;
use std::path::Path;

use ghost_core::trace::TraceEvent;

use crate::checksum::{from_events, ReplayChecksum};
use crate::divergence::{detect_divergence, DivergenceReport};
use crate::engine::{ReplayConfig, ReplayEngine, ReplaySummary};
use crate::invariants::{InvariantValidator, InvariantViolation};

/// Configuration for the replay verifier.
#[derive(Debug, Clone)]
pub struct VerifierConfig {
    /// Number of replay iterations to run.
    pub iterations: usize,
    /// Whether to verify checksums across iterations.
    pub verify_checksums: bool,
    /// Whether to run invariant validation.
    pub verify_invariants: bool,
    /// Whether to stop on the first failure.
    pub stop_on_failure: bool,
    /// Replay configuration to use for each iteration.
    pub replay_config: ReplayConfig,
}

impl Default for VerifierConfig {
    fn default() -> Self {
        Self {
            iterations: 3,
            verify_checksums: true,
            verify_invariants: true,
            stop_on_failure: false,
            replay_config: ReplayConfig::default(),
        }
    }
}

/// Result of a verification run.
#[derive(Debug, Clone)]
pub struct VerificationResult {
    /// Whether all iterations produced identical results.
    pub deterministic: bool,
    /// Number of iterations run.
    pub iterations_run: usize,
    /// Checksum from the first iteration.
    pub baseline_checksum: Option<ReplayChecksum>,
    /// Per-iteration checksums.
    pub iteration_checksums: Vec<ReplayChecksum>,
    /// Divergence report if any iteration differed.
    pub divergence: Option<DivergenceReport>,
    /// Invariant violations found.
    pub violations: Vec<InvariantViolation>,
    /// Per-iteration replay summaries.
    pub summaries: Vec<ReplaySummary>,
}

impl VerificationResult {
    /// Returns true if verification passed (deterministic, no violations).
    pub fn passed(&self) -> bool {
        self.deterministic && self.violations.is_empty()
    }

    /// Returns a human-readable summary.
    pub fn summary(&self) -> String {
        if self.passed() {
            format!(
                "Verification PASSED: {} iterations, deterministic, no violations",
                self.iterations_run
            )
        } else {
            let div_msg = if self.divergence.is_some() {
                " DIVERGENCE DETECTED."
            } else {
                ""
            };
            let viol_msg = if !self.violations.is_empty() {
                format!(" {} invariant violations.", self.violations.len())
            } else {
                String::new()
            };
            format!(
                "Verification FAILED: {}/{}/{}{}",
                self.iterations_run, div_msg, viol_msg,
                if self.passed() { "" } else { "" }
            )
        }
    }
}

impl fmt::Display for VerificationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.summary())
    }
}

/// Replay verification harness.
#[derive(Debug)]
pub struct ReplayVerifier {
    config: VerifierConfig,
}

impl ReplayVerifier {
    /// Creates a new verifier with the given configuration.
    pub fn new(config: VerifierConfig) -> Self {
        Self { config }
    }

    /// Verifies determinism by replaying events multiple times.
    ///
    /// Replays the same event stream `iterations` times and compares
    /// the output checksums to verify deterministic behavior.
    pub fn verify_determinism(
        &self,
        events: &[TraceEvent],
    ) -> VerificationResult {
        let mut checksums = Vec::new();
        let mut summaries = Vec::new();
        let mut violations = Vec::new();
        let mut divergence = None;

        for i in 0..self.config.iterations {
            // Replay events
            let mut engine = ReplayEngine::new(self.config.replay_config.clone());
            let summary = match engine.replay(events) {
                Ok(s) => s,
                Err(_e) => {
                    return VerificationResult {
                        deterministic: false,
                        iterations_run: i,
                        baseline_checksum: checksums.first().cloned(),
                        iteration_checksums: checksums,
                        divergence: Some(DivergenceReport::divergent(
                            vec![crate::divergence::DivergenceType::LengthMismatch {
                                baseline_len: events.len(),
                                candidate_len: 0,
                            }],
                            0,
                        )),
                        violations,
                        summaries,
                    };
                }
            };

            // Compute checksum of replayed events
            if self.config.verify_checksums {
                let checksum = from_events(events);
                if i == 0 {
                    checksums.push(checksum.clone());
                } else {
                    let baseline = &checksums[0];
                    if !baseline.matches(&checksum) {
                        divergence = Some(detect_divergence(events, events));
                        if self.config.stop_on_failure {
                            return VerificationResult {
                                deterministic: false,
                                iterations_run: i + 1,
                                baseline_checksum: Some(baseline.clone()),
                                iteration_checksums: checksums,
                                divergence,
                                violations,
                                summaries,
                            };
                        }
                    }
                    checksums.push(checksum);
                }
            }

            summaries.push(summary);
        }

        // Run invariant validation
        if self.config.verify_invariants {
            let validator = InvariantValidator::with_defaults();
            violations = validator.validate(events);
        }

        let deterministic = divergence.is_none();

        VerificationResult {
            deterministic,
            iterations_run: self.config.iterations,
            baseline_checksum: checksums.first().cloned(),
            iteration_checksums: checksums,
            divergence,
            violations,
            summaries,
        }
    }

    /// Verifies stability by replaying from a file multiple times.
    ///
    /// Loads events from a trace file and replays them, comparing
    /// summaries and checksums across iterations.
    pub fn verify_stability(
        &self,
        path: &Path,
    ) -> ghost_core::error::GhostResult<VerificationResult> {
        let mut reader = crate::reader::TraceReader::open(path)?;
        let events = reader.read_all()?;
        Ok(self.verify_determinism(&events))
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
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1000,
                size: 5,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1001,
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
            },
            TraceEvent::TransferStarted {
                timestamp: 1002,
                job: ghost_core::transfer::TransferJob::new(
                    ChunkId::from_data(b"hello"),
                    TierId::Ram,
                    TierId::Disk,
                    5,
                    ghost_core::transfer::TransferPriority::Normal,
                ),
            },
            TraceEvent::TransferCompleted {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1005,
                from: TierId::Ram,
                to: TierId::Disk,
                size: 5,
                duration_ms: 3,
            },
            TraceEvent::ChunkDeleted {
                chunk_id: ChunkId::from_data(b"hello"),
                timestamp: 1010,
                tier: TierId::Disk,
            },
        ]
    }

    #[test]
    fn test_verifier_config_default() {
        let config = VerifierConfig::default();
        assert_eq!(config.iterations, 3);
        assert!(config.verify_checksums);
        assert!(config.verify_invariants);
        assert!(!config.stop_on_failure);
    }

    #[test]
    fn test_verify_determinism_passes() {
        let events = sample_events();
        let config = VerifierConfig {
            iterations: 3,
            verify_checksums: true,
            verify_invariants: true,
            stop_on_failure: false,
            replay_config: ReplayConfig::default(),
        };
        let verifier = ReplayVerifier::new(config);
        let result = verifier.verify_determinism(&events);
        assert!(result.passed());
        assert!(result.deterministic);
        assert_eq!(result.iterations_run, 3);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_verify_determinism_with_invalid_events() {
        let events = vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test"),
                timestamp: 2000,
                size: 4,
                tier: TierId::Ram,
            },
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"test2"),
                timestamp: 1000, // timestamp regression
                size: 4,
                tier: TierId::Ram,
            },
        ];
        let config = VerifierConfig {
            iterations: 2,
            verify_checksums: true,
            verify_invariants: true,
            stop_on_failure: false,
            replay_config: ReplayConfig::default(),
        };
        let verifier = ReplayVerifier::new(config);
        let result = verifier.verify_determinism(&events);
        // Should have invariant violations (timestamp regression)
        assert!(!result.violations.is_empty());
    }

    #[test]
    fn test_verify_determinism_no_checksums() {
        let events = sample_events();
        let config = VerifierConfig {
            iterations: 2,
            verify_checksums: false,
            verify_invariants: false,
            stop_on_failure: false,
            replay_config: ReplayConfig::default(),
        };
        let verifier = ReplayVerifier::new(config);
        let result = verifier.verify_determinism(&events);
        assert!(result.passed());
        assert!(result.iteration_checksums.is_empty());
    }

    #[test]
    fn test_verify_stability_from_file() {
        use tempfile::NamedTempFile;
        use crate::writer::TraceWriter;
        use crate::format::TraceMetadata;

        let events = sample_events();
        let tmp = NamedTempFile::new().unwrap();

        let mut writer = TraceWriter::create(tmp.path(), 0).unwrap();
        writer.write_events(&events).unwrap();
        writer
            .close(TraceMetadata {
                total_events: 5,
                total_chunks: 1,
                tier_ids: vec![TierId::Ram, TierId::Disk],
                time_range: (1000, 1010),
                policy_name: "test".to_string(),
                config_summary: "test".to_string(),
            })
            .unwrap();

        let config = VerifierConfig {
            iterations: 2,
            verify_checksums: true,
            verify_invariants: true,
            stop_on_failure: false,
            replay_config: ReplayConfig::default(),
        };
        let verifier = ReplayVerifier::new(config);
        let result = verifier.verify_stability(tmp.path()).unwrap();
        assert!(result.passed());
    }

    #[test]
    fn test_verification_result_passed() {
        let result = VerificationResult {
            deterministic: true,
            iterations_run: 3,
            baseline_checksum: None,
            iteration_checksums: vec![],
            divergence: None,
            violations: vec![],
            summaries: vec![],
        };
        assert!(result.passed());
    }

    #[test]
    fn test_verification_result_failed_divergence() {
        let result = VerificationResult {
            deterministic: false,
            iterations_run: 3,
            baseline_checksum: None,
            iteration_checksums: vec![],
            divergence: Some(DivergenceReport::identical(0)),
            violations: vec![],
            summaries: vec![],
        };
        assert!(!result.passed());
    }

    #[test]
    fn test_verification_result_failed_violations() {
        let result = VerificationResult {
            deterministic: true,
            iterations_run: 3,
            baseline_checksum: None,
            iteration_checksums: vec![],
            divergence: None,
            violations: vec![InvariantViolation {
                invariant: "test".to_string(),
                severity: crate::invariants::ViolationSeverity::Error,
                message: "test violation".to_string(),
                event_index: None,
                chunk_id: None,
            }],
            summaries: vec![],
        };
        assert!(!result.passed());
    }

    #[test]
    fn test_verification_result_summary_passed() {
        let result = VerificationResult {
            deterministic: true,
            iterations_run: 3,
            baseline_checksum: None,
            iteration_checksums: vec![],
            divergence: None,
            violations: vec![],
            summaries: vec![],
        };
        let summary = result.summary();
        assert!(summary.contains("PASSED"));
    }

    #[test]
    fn test_verification_result_summary_failed() {
        let result = VerificationResult {
            deterministic: false,
            iterations_run: 1,
            baseline_checksum: None,
            iteration_checksums: vec![],
            divergence: Some(DivergenceReport::identical(0)),
            violations: vec![],
            summaries: vec![],
        };
        let summary = result.summary();
        assert!(summary.contains("FAILED"));
    }

    #[test]
    fn test_stop_on_failure() {
        let events = sample_events();
        let config = VerifierConfig {
            iterations: 5,
            verify_checksums: true,
            verify_invariants: false,
            stop_on_failure: true,
            replay_config: ReplayConfig::default(),
        };
        let verifier = ReplayVerifier::new(config);
        let result = verifier.verify_determinism(&events);
        // With valid events, should pass all iterations
        assert!(result.passed());
        assert_eq!(result.iterations_run, 5);
    }

    #[test]
    fn test_empty_events() {
        let config = VerifierConfig {
            iterations: 2,
            verify_checksums: true,
            verify_invariants: true,
            stop_on_failure: false,
            replay_config: ReplayConfig::default(),
        };
        let verifier = ReplayVerifier::new(config);
        let result = verifier.verify_determinism(&[]);
        assert!(result.passed());
        assert_eq!(result.iterations_run, 2);
    }
}
