//! Diagnostic snapshot for the GhostPages daemon.
//!
//! Provides a comprehensive view of system health, including queue status,
//! migration status, allocator status, backend health, and replay status.
//! The snapshot is JSON-serializable for easy consumption by monitoring tools.

use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use ghost_core::state::PressureState;

/// Comprehensive diagnostic snapshot of the daemon's health.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticSnapshot {
    /// Unix timestamp when the snapshot was taken.
    pub timestamp: u64,
    /// Uptime of the daemon in seconds.
    pub uptime_secs: u64,
    /// Overall health assessment.
    pub overall_health: HealthStatus,
    /// Queue diagnostics.
    pub queue: QueueDiagnostics,
    /// Migration diagnostics.
    pub migration: MigrationDiagnostics,
    /// Allocator diagnostics.
    pub allocator: AllocatorDiagnostics,
    /// Per-backend health diagnostics.
    pub backends: HashMap<String, BackendDiagnostics>,
    /// Replay diagnostics.
    pub replay: ReplayDiagnostics,
    /// Current pressure state.
    pub pressure: PressureState,
}

/// Overall health status assessment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// All systems operating normally.
    Healthy,
    /// One or more subsystems degraded but functional.
    Degraded,
    /// Critical failure in one or more subsystems.
    Unhealthy,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Queue subsystem diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDiagnostics {
    /// Current depth of the transfer queue.
    pub depth: usize,
    /// Maximum queue capacity.
    pub capacity: usize,
    /// Whether the queue is full.
    pub is_full: bool,
    /// Whether the queue is shut down.
    pub is_shutdown: bool,
    /// Total jobs submitted since startup.
    pub submitted_total: u64,
    /// Total jobs dequeued since startup.
    pub dequeued_total: u64,
    /// Total priority insertions.
    pub priority_insertions_total: u64,
    /// Total rejected submissions.
    pub rejected_total: u64,
}

/// Migration subsystem diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationDiagnostics {
    /// Total evaluation cycles run.
    pub evaluation_cycles_total: u64,
    /// Total promotions executed.
    pub promotions_total: u64,
    /// Total evictions executed.
    pub evictions_total: u64,
    /// Total skipped migrations.
    pub skipped_total: u64,
    /// Total migration failures.
    pub failures_total: u64,
    /// Total bytes migrated.
    pub bytes_migrated_total: u64,
    /// Currently active migrations.
    pub active_migrations: u64,
    /// Total pending migrations identified.
    pub pending_identified_total: u64,
}

/// Allocator subsystem diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatorDiagnostics {
    /// Total allocations performed.
    pub allocations_total: u64,
    /// Total deallocations performed.
    pub deallocations_total: u64,
    /// Total allocation failures.
    pub allocation_failures_total: u64,
    /// Currently allocated bytes.
    pub allocated_bytes: u64,
    /// Peak allocated bytes since startup.
    pub peak_allocated_bytes: u64,
    /// Total bytes allocated over lifetime.
    pub bytes_allocated_total: u64,
    /// Total bytes deallocated over lifetime.
    pub bytes_deallocated_total: u64,
    /// Currently active allocations.
    pub active_allocations: u64,
}

/// Per-backend health diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendDiagnostics {
    /// Tier identifier.
    pub tier_id: String,
    /// Current health status.
    pub health: String,
    /// Total successful health checks.
    pub health_check_successes_total: u64,
    /// Total failed health checks.
    pub health_check_failures_total: u64,
    /// Total degradation events.
    pub degradation_events_total: u64,
    /// Total recovery events.
    pub recovery_events_total: u64,
    /// Total recovery attempts.
    pub recovery_attempts_total: u64,
    /// Total successful recoveries.
    pub recovery_successes_total: u64,
    /// Current consecutive failure count.
    pub consecutive_failures: u64,
}

/// Replay subsystem diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDiagnostics {
    /// Total replay operations performed.
    pub replay_ops_total: u64,
    /// Total events replayed.
    pub events_replayed_total: u64,
    /// Total validation errors encountered.
    pub validation_errors_total: u64,
    /// Total replay failures.
    pub failures_total: u64,
    /// Currently active replays.
    pub active_replays: u64,
    /// Total determinism checks performed.
    pub determinism_checks_total: u64,
    /// Total determinism check failures.
    pub determinism_failures_total: u64,
}

impl Default for DiagnosticSnapshot {
    fn default() -> Self {
        Self {
            timestamp: 0,
            uptime_secs: 0,
            overall_health: HealthStatus::Healthy,
            queue: QueueDiagnostics::default(),
            migration: MigrationDiagnostics::default(),
            allocator: AllocatorDiagnostics::default(),
            backends: HashMap::new(),
            replay: ReplayDiagnostics::default(),
            pressure: PressureState::new(),
        }
    }
}

impl Default for QueueDiagnostics {
    fn default() -> Self {
        Self {
            depth: 0,
            capacity: 0,
            is_full: false,
            is_shutdown: false,
            submitted_total: 0,
            dequeued_total: 0,
            priority_insertions_total: 0,
            rejected_total: 0,
        }
    }
}

impl Default for MigrationDiagnostics {
    fn default() -> Self {
        Self {
            evaluation_cycles_total: 0,
            promotions_total: 0,
            evictions_total: 0,
            skipped_total: 0,
            failures_total: 0,
            bytes_migrated_total: 0,
            active_migrations: 0,
            pending_identified_total: 0,
        }
    }
}

impl Default for AllocatorDiagnostics {
    fn default() -> Self {
        Self {
            allocations_total: 0,
            deallocations_total: 0,
            allocation_failures_total: 0,
            allocated_bytes: 0,
            peak_allocated_bytes: 0,
            bytes_allocated_total: 0,
            bytes_deallocated_total: 0,
            active_allocations: 0,
        }
    }
}

impl Default for ReplayDiagnostics {
    fn default() -> Self {
        Self {
            replay_ops_total: 0,
            events_replayed_total: 0,
            validation_errors_total: 0,
            failures_total: 0,
            active_replays: 0,
            determinism_checks_total: 0,
            determinism_failures_total: 0,
        }
    }
}

impl Default for BackendDiagnostics {
    fn default() -> Self {
        Self {
            tier_id: String::new(),
            health: "unknown".to_string(),
            health_check_successes_total: 0,
            health_check_failures_total: 0,
            degradation_events_total: 0,
            recovery_events_total: 0,
            recovery_attempts_total: 0,
            recovery_successes_total: 0,
            consecutive_failures: 0,
        }
    }
}

/// Builder for constructing a `DiagnosticSnapshot` from live system state.
pub struct DiagnosticSnapshotBuilder {
    start_time: Instant,
}

impl DiagnosticSnapshotBuilder {
    /// Create a new diagnostic snapshot builder.
    pub fn new(start_time: Instant) -> Self {
        Self { start_time }
    }

    /// Build a diagnostic snapshot with default values.
    ///
    /// In a production system, this would pull live metrics from the
    /// orchestrator, health tracker, and other subsystems. For now, it
    /// returns a baseline snapshot that demonstrates the structure.
    pub fn build_default(&self) -> DiagnosticSnapshot {
        DiagnosticSnapshot {
            timestamp: ghost_core::trace::current_timestamp(),
            uptime_secs: self.start_time.elapsed().as_secs(),
            overall_health: HealthStatus::Healthy,
            queue: QueueDiagnostics::default(),
            migration: MigrationDiagnostics::default(),
            allocator: AllocatorDiagnostics::default(),
            backends: HashMap::new(),
            replay: ReplayDiagnostics::default(),
            pressure: PressureState::new(),
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diagnostic_snapshot_default() {
        let snapshot = DiagnosticSnapshot::default();
        assert_eq!(snapshot.overall_health, HealthStatus::Healthy);
        assert!(snapshot.backends.is_empty());
        assert_eq!(snapshot.queue.depth, 0);
        assert_eq!(snapshot.migration.active_migrations, 0);
    }

    #[test]
    fn test_diagnostic_snapshot_serialization() {
        let snapshot = DiagnosticSnapshot::default();
        let json = serde_json::to_string(&snapshot).expect("should serialize");
        let deserialized: DiagnosticSnapshot =
            serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(deserialized.overall_health, snapshot.overall_health);
    }

    #[test]
    fn test_health_status_display() {
        assert_eq!(format!("{}", HealthStatus::Healthy), "healthy");
        assert_eq!(format!("{}", HealthStatus::Degraded), "degraded");
        assert_eq!(format!("{}", HealthStatus::Unhealthy), "unhealthy");
    }

    #[test]
    fn test_snapshot_builder() {
        let builder = DiagnosticSnapshotBuilder::new(Instant::now());
        let snapshot = builder.build_default();
        assert_eq!(snapshot.overall_health, HealthStatus::Healthy);
    }

    #[test]
    fn test_backend_diagnostics_default() {
        let diag = BackendDiagnostics::default();
        assert_eq!(diag.health, "unknown");
        assert_eq!(diag.consecutive_failures, 0);
    }
}
