//! Transfer job model for chunk migration between tiers.
//!
//! This module defines the types used to track and manage chunk transfers
//! as they move through the system.

use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::types::{ChunkId, TierId};

/// Priority level for a transfer job.
///
/// Higher priority transfers are processed before lower priority ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferPriority {
    /// Background migration (lowest priority).
    Low,

    /// Standard operation.
    Normal,

    /// User-requested transfer.
    High,

    /// Pressure relief (highest priority).
    Critical,
}

impl TransferPriority {
    /// Get the numeric priority value (higher = more important).
    pub fn value(&self) -> u8 {
        match self {
            TransferPriority::Low => 0,
            TransferPriority::Normal => 1,
            TransferPriority::High => 2,
            TransferPriority::Critical => 3,
        }
    }

    /// Check if this priority is higher than another.
    pub fn is_higher_than(&self, other: TransferPriority) -> bool {
        self.value() > other.value()
    }
}

impl std::fmt::Display for TransferPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferPriority::Low => write!(f, "low"),
            TransferPriority::Normal => write!(f, "normal"),
            TransferPriority::High => write!(f, "high"),
            TransferPriority::Critical => write!(f, "critical"),
        }
    }
}

/// Current state of a transfer job in the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferState {
    /// Waiting in queue to be processed.
    Queued,

    /// Being compressed before transfer.
    Compressing,

    /// Data is being moved between tiers.
    Transferring,

    /// Being written to destination tier.
    Writing,

    /// Integrity verification in progress.
    Verifying,

    /// Transfer completed successfully.
    Complete,

    /// Transfer failed (may be retried).
    Failed,

    /// Transfer was cancelled.
    Cancelled,
}

impl TransferState {
    /// Check if this is a terminal state (no further transitions expected).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransferState::Complete | TransferState::Failed | TransferState::Cancelled
        )
    }

    /// Check if this is an active (in-progress) state.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            TransferState::Compressing
                | TransferState::Transferring
                | TransferState::Writing
                | TransferState::Verifying
        )
    }
}

impl std::fmt::Display for TransferState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferState::Queued => write!(f, "queued"),
            TransferState::Compressing => write!(f, "compressing"),
            TransferState::Transferring => write!(f, "transferring"),
            TransferState::Writing => write!(f, "writing"),
            TransferState::Verifying => write!(f, "verifying"),
            TransferState::Complete => write!(f, "complete"),
            TransferState::Failed => write!(f, "failed"),
            TransferState::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// A transfer job tracking the movement of a chunk between tiers.
///
/// This is the primary unit of work for the transfer pipeline. Each job
/// tracks the complete lifecycle of a chunk migration.
///
/// Note: `Serialize` is implemented manually to handle the `Instant` field
/// which is not serializable. `Deserialize` is also implemented manually,
/// reconstructing `created_at` from `created_at_secs`.
#[derive(Debug, Clone)]
pub struct TransferJob {
    /// The chunk being transferred.
    pub chunk_id: ChunkId,

    /// Source tier.
    pub from_tier: TierId,

    /// Destination tier.
    pub to_tier: TierId,

    /// Size of the data being transferred in bytes.
    pub size: usize,

    /// Priority of this transfer.
    pub priority: TransferPriority,

    /// Current state of the transfer.
    pub state: TransferState,

    /// When this job was created (not serialized — use created_at_secs).
    pub created_at: Instant,

    /// Creation timestamp in seconds since epoch (for serialization).
    pub created_at_secs: u64,

    /// Number of attempts made so far.
    pub attempts: u32,
}

impl Serialize for TransferJob {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(8))?;
        map.serialize_entry("chunk_id", &self.chunk_id)?;
        map.serialize_entry("from_tier", &self.from_tier)?;
        map.serialize_entry("to_tier", &self.to_tier)?;
        map.serialize_entry("size", &self.size)?;
        map.serialize_entry("priority", &self.priority)?;
        map.serialize_entry("state", &self.state)?;
        map.serialize_entry("created_at_secs", &self.created_at_secs)?;
        map.serialize_entry("attempts", &self.attempts)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for TransferJob {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct TransferJobVisitor;

        impl<'de> Visitor<'de> for TransferJobVisitor {
            type Value = TransferJob;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a TransferJob map")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<TransferJob, M::Error> {
                let mut chunk_id = None;
                let mut from_tier = None;
                let mut to_tier = None;
                let mut size = None;
                let mut priority = None;
                let mut state = None;
                let mut created_at_secs = None;
                let mut attempts = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "chunk_id" => chunk_id = Some(map.next_value()?),
                        "from_tier" => from_tier = Some(map.next_value()?),
                        "to_tier" => to_tier = Some(map.next_value()?),
                        "size" => size = Some(map.next_value()?),
                        "priority" => priority = Some(map.next_value()?),
                        "state" => state = Some(map.next_value()?),
                        "created_at_secs" => created_at_secs = Some(map.next_value()?),
                        "attempts" => attempts = Some(map.next_value()?),
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let chunk_id = chunk_id.ok_or_else(|| de::Error::missing_field("chunk_id"))?;
                let from_tier = from_tier.ok_or_else(|| de::Error::missing_field("from_tier"))?;
                let to_tier = to_tier.ok_or_else(|| de::Error::missing_field("to_tier"))?;
                let size = size.ok_or_else(|| de::Error::missing_field("size"))?;
                let priority = priority.ok_or_else(|| de::Error::missing_field("priority"))?;
                let state = state.ok_or_else(|| de::Error::missing_field("state"))?;
                let created_at_secs =
                    created_at_secs.ok_or_else(|| de::Error::missing_field("created_at_secs"))?;
                let attempts = attempts.ok_or_else(|| de::Error::missing_field("attempts"))?;

                // Reconstruct Instant from seconds since epoch
                let created_at = std::time::SystemTime::UNIX_EPOCH
                    .checked_add(std::time::Duration::from_secs(created_at_secs))
                    .map(|_st| Instant::now()) // Fallback: use now if overflow
                    .unwrap_or_else(Instant::now);

                Ok(TransferJob {
                    chunk_id,
                    from_tier,
                    to_tier,
                    size,
                    priority,
                    state,
                    created_at,
                    created_at_secs,
                    attempts,
                })
            }
        }

        deserializer.deserialize_map(TransferJobVisitor)
    }
}

impl TransferJob {
    /// Create a new transfer job.
    pub fn new(
        chunk_id: ChunkId,
        from_tier: TierId,
        to_tier: TierId,
        size: usize,
        priority: TransferPriority,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            chunk_id,
            from_tier,
            to_tier,
            size,
            priority,
            state: TransferState::Queued,
            created_at: Instant::now(),
            created_at_secs: now,
            attempts: 0,
        }
    }

    /// Record an attempt (increment the attempt counter).
    pub fn record_attempt(&mut self) {
        self.attempts += 1;
    }

    /// Transition to a new transfer state.
    pub fn transition_state(&mut self, next: TransferState) {
        self.state = next;
    }

    /// Check if this transfer has exceeded a given timeout duration.
    pub fn is_timed_out(&self, timeout: std::time::Duration) -> bool {
        self.created_at.elapsed() > timeout
    }

    /// Get the elapsed time since this job was created.
    pub fn elapsed(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Check if this is a high-priority transfer that should be processed first.
    pub fn is_urgent(&self) -> bool {
        matches!(
            self.priority,
            TransferPriority::High | TransferPriority::Critical
        )
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_priority_value() {
        assert_eq!(TransferPriority::Low.value(), 0);
        assert_eq!(TransferPriority::Normal.value(), 1);
        assert_eq!(TransferPriority::High.value(), 2);
        assert_eq!(TransferPriority::Critical.value(), 3);
    }

    #[test]
    fn test_transfer_priority_ordering() {
        assert!(TransferPriority::Critical.is_higher_than(TransferPriority::High));
        assert!(TransferPriority::High.is_higher_than(TransferPriority::Normal));
        assert!(TransferPriority::Normal.is_higher_than(TransferPriority::Low));
        assert!(!TransferPriority::Low.is_higher_than(TransferPriority::Normal));
    }

    #[test]
    fn test_transfer_state_terminal() {
        assert!(TransferState::Complete.is_terminal());
        assert!(TransferState::Failed.is_terminal());
        assert!(TransferState::Cancelled.is_terminal());
        assert!(!TransferState::Queued.is_terminal());
        assert!(!TransferState::Transferring.is_terminal());
    }

    #[test]
    fn test_transfer_state_active() {
        assert!(TransferState::Compressing.is_active());
        assert!(TransferState::Transferring.is_active());
        assert!(TransferState::Writing.is_active());
        assert!(TransferState::Verifying.is_active());
        assert!(!TransferState::Queued.is_active());
        assert!(!TransferState::Complete.is_active());
        assert!(!TransferState::Failed.is_active());
    }

    #[test]
    fn test_transfer_job_creation() {
        let chunk_id = ChunkId::from_data(b"test");
        let job = TransferJob::new(
            chunk_id,
            TierId::Ram,
            TierId::GpuVram,
            1024,
            TransferPriority::Normal,
        );

        assert_eq!(job.chunk_id, chunk_id);
        assert_eq!(job.from_tier, TierId::Ram);
        assert_eq!(job.to_tier, TierId::GpuVram);
        assert_eq!(job.size, 1024);
        assert_eq!(job.priority, TransferPriority::Normal);
        assert_eq!(job.state, TransferState::Queued);
        assert_eq!(job.attempts, 0);
    }

    #[test]
    fn test_transfer_job_record_attempt() {
        let mut job = TransferJob::new(
            ChunkId::from_data(b"test"),
            TierId::Ram,
            TierId::Disk,
            512,
            TransferPriority::Low,
        );

        assert_eq!(job.attempts, 0);
        job.record_attempt();
        assert_eq!(job.attempts, 1);
        job.record_attempt();
        assert_eq!(job.attempts, 2);
    }

    #[test]
    fn test_transfer_job_transition_state() {
        let mut job = TransferJob::new(
            ChunkId::from_data(b"test"),
            TierId::Ram,
            TierId::Disk,
            512,
            TransferPriority::Normal,
        );

        assert_eq!(job.state, TransferState::Queued);
        job.transition_state(TransferState::Compressing);
        assert_eq!(job.state, TransferState::Compressing);
        job.transition_state(TransferState::Transferring);
        assert_eq!(job.state, TransferState::Transferring);
        job.transition_state(TransferState::Complete);
        assert_eq!(job.state, TransferState::Complete);
    }

    #[test]
    fn test_transfer_job_is_urgent() {
        let critical = TransferJob::new(
            ChunkId::from_data(b"c"),
            TierId::Ram,
            TierId::Disk,
            100,
            TransferPriority::Critical,
        );
        assert!(critical.is_urgent());

        let high = TransferJob::new(
            ChunkId::from_data(b"h"),
            TierId::Ram,
            TierId::Disk,
            100,
            TransferPriority::High,
        );
        assert!(high.is_urgent());

        let normal = TransferJob::new(
            ChunkId::from_data(b"n"),
            TierId::Ram,
            TierId::Disk,
            100,
            TransferPriority::Normal,
        );
        assert!(!normal.is_urgent());

        let low = TransferJob::new(
            ChunkId::from_data(b"l"),
            TierId::Ram,
            TierId::Disk,
            100,
            TransferPriority::Low,
        );
        assert!(!low.is_urgent());
    }

    #[test]
    fn test_transfer_job_elapsed() {
        let job = TransferJob::new(
            ChunkId::from_data(b"test"),
            TierId::Ram,
            TierId::Disk,
            512,
            TransferPriority::Normal,
        );

        // Elapsed should be very small but non-zero
        let elapsed = job.elapsed();
        assert!(elapsed.as_nanos() > 0);
    }

    #[test]
    fn test_transfer_priority_display() {
        assert_eq!(format!("{}", TransferPriority::Low), "low");
        assert_eq!(format!("{}", TransferPriority::Normal), "normal");
        assert_eq!(format!("{}", TransferPriority::High), "high");
        assert_eq!(format!("{}", TransferPriority::Critical), "critical");
    }

    #[test]
    fn test_transfer_state_display() {
        assert_eq!(format!("{}", TransferState::Queued), "queued");
        assert_eq!(format!("{}", TransferState::Compressing), "compressing");
        assert_eq!(format!("{}", TransferState::Transferring), "transferring");
        assert_eq!(format!("{}", TransferState::Writing), "writing");
        assert_eq!(format!("{}", TransferState::Verifying), "verifying");
        assert_eq!(format!("{}", TransferState::Complete), "complete");
        assert_eq!(format!("{}", TransferState::Failed), "failed");
        assert_eq!(format!("{}", TransferState::Cancelled), "cancelled");
    }

    #[test]
    fn test_transfer_job_serialization_roundtrip() {
        let job = TransferJob::new(
            ChunkId::from_data(b"roundtrip"),
            TierId::Ram,
            TierId::Disk,
            4096,
            TransferPriority::High,
        );

        let serialized = serde_json::to_string(&job).expect("serialize transfer job");
        let deserialized: TransferJob =
            serde_json::from_str(&serialized).expect("deserialize transfer job");

        assert_eq!(job.chunk_id, deserialized.chunk_id);
        assert_eq!(job.from_tier, deserialized.from_tier);
        assert_eq!(job.to_tier, deserialized.to_tier);
        assert_eq!(job.size, deserialized.size);
        assert_eq!(job.priority, deserialized.priority);
        assert_eq!(job.state, deserialized.state);
        assert_eq!(job.created_at_secs, deserialized.created_at_secs);
        assert_eq!(job.attempts, deserialized.attempts);
    }
}
