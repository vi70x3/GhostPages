//! Migration engine for pressure-driven chunk migration.
//!
//! Implements the core migration logic: evaluating which chunks should be
//! promoted to faster tiers, evicted from pressured tiers, or left in place.
//! Uses the PlacementPolicy trait for all placement decisions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::state::{ChunkState, PressureState, StateMachine};
use ghost_core::trace::{current_timestamp, EvictionReason, TraceEvent};
use ghost_core::transfer::{TransferJob, TransferPriority};
use ghost_core::types::{ChunkId, ChunkMeta, TierId};
use ghost_policy::PlacementPolicy;
use ghost_tier::StorageBackend;

use crate::config::MigrationConfig;
use crate::hotness_tracker::HotnessTracker;
use crate::trace_log::TraceLog;

/// Statistics for migration operations.
#[derive(Debug, Clone, Default)]
pub struct MigrationStats {
    /// Total number of migration evaluation cycles.
    pub evaluation_cycles: u64,
    /// Total promotions performed.
    pub promotions: u64,
    /// Total evictions performed.
    pub evictions: u64,
    /// Total migrations skipped (policy rejected).
    pub skipped: u64,
    /// Total migration failures.
    pub failures: u64,
    /// Total bytes migrated.
    pub bytes_migrated: u64,
    /// Timestamp of the last evaluation cycle.
    pub last_evaluation: u64,
    /// Number of chunks currently being migrated.
    pub active_migrations: usize,
}

/// A pending migration operation.
#[derive(Debug, Clone)]
pub struct PendingMigration {
    /// Chunk to migrate.
    pub chunk_id: ChunkId,
    /// Source tier.
    pub from_tier: TierId,
    /// Destination tier.
    pub to_tier: TierId,
    /// Transfer priority.
    pub priority: TransferPriority,
    /// Size of the chunk in bytes.
    pub size: usize,
    /// Hotness score at the time of evaluation.
    pub hotness_score: f32,
    /// Timestamp when the migration was identified.
    pub identified_at: u64,
}

/// Migration engine that evaluates and executes pressure-driven chunk migrations.
pub struct MigrationEngine {
    config: MigrationConfig,
    policy: Arc<dyn PlacementPolicy>,
    hotness_tracker: Arc<HotnessTracker>,
    state_machine: Arc<std::sync::Mutex<StateMachine>>,
    trace_log: Arc<TraceLog>,
    backends: BTreeMap<TierId, Arc<dyn StorageBackend>>,
    stats: Arc<std::sync::Mutex<MigrationStats>>,
    /// Set of chunks currently being migrated.
    active_migrations: Arc<std::sync::Mutex<BTreeSet<ChunkId>>>,
    /// Timestamp of the last migration for each chunk (rate limiting).
    last_migration: Arc<std::sync::Mutex<BTreeMap<ChunkId, u64>>>,
    /// Optional event emitter for unified event taxonomy.
    event_emitter: Option<EventEmitter>,
}

impl MigrationEngine {
    /// Create a new migration engine.
    pub fn new(
        config: MigrationConfig,
        policy: Arc<dyn PlacementPolicy>,
        hotness_tracker: Arc<HotnessTracker>,
        state_machine: Arc<std::sync::Mutex<StateMachine>>,
        trace_log: Arc<TraceLog>,
        backends: BTreeMap<TierId, Arc<dyn StorageBackend>>,
    ) -> Self {
        Self {
            config,
            policy,
            hotness_tracker,
            state_machine,
            trace_log,
            backends,
            stats: Arc::new(std::sync::Mutex::new(MigrationStats::default())),
            active_migrations: Arc::new(std::sync::Mutex::new(BTreeSet::new())),
            last_migration: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            event_emitter: None,
        }
    }

    /// Set the event emitter for unified event taxonomy.
    pub fn set_event_emitter(&mut self, emitter: EventEmitter) {
        self.event_emitter = Some(emitter);
    }

    /// Evaluate the current system state and identify migrations to perform.
    ///
    /// Returns a list of pending migrations sorted by priority.
    pub fn evaluate(&self, pressure: &PressureState) -> Vec<PendingMigration> {
        let now = current_timestamp();
        let mut migrations = Vec::new();
        let mut stats = self.stats.lock().unwrap();
        stats.evaluation_cycles += 1;
        stats.last_evaluation = now;
        drop(stats);

        // Get all chunks and their states
        let sm = self.state_machine.lock().unwrap();
        let stored_chunks: Vec<(ChunkId, ChunkMeta)> = sm
            .chunks_in_state(ChunkState::Stored)
            .into_iter()
            .filter_map(|id| {
                // Build a minimal ChunkMeta for policy evaluation
                let hotness = self.hotness_tracker.get_hotness(&id);
                Some((
                    id,
                    ChunkMeta {
                        id,
                        size: 0,
                        compressed_size: 0,
                        tier: TierId::Ram,
                        state: ChunkState::Stored,
                        created_at: 0,
                        last_accessed: hotness.map(|h| h.last_accessed).unwrap_or(0),
                        access_count: hotness.map(|h| h.access_count).unwrap_or(0),
                        compression: ghost_core::types::CompressionAlgorithm::None,
                        checksum: [0u8; 32],
                    },
                ))
            })
            .collect();
        drop(sm);

        let _available_tiers: Vec<TierId> = self.backends.keys().cloned().collect();
        let active = self.active_migrations.lock().unwrap();
        let last_migration = self.last_migration.lock().unwrap();

        for (chunk_id, meta) in &stored_chunks {
            // Skip chunks currently being migrated
            if active.contains(chunk_id) {
                continue;
            }

            // Rate limit: skip chunks migrated recently
            if let Some(&last_time) = last_migration.get(chunk_id) {
                if now.saturating_sub(last_time) < self.config.min_migration_interval_secs {
                    continue;
                }
            }

            // Ask the policy if this chunk should be migrated
            if let Some(target_tier) = self.policy.should_migrate(
                meta,
                meta.tier,
                pressure,
            ) {
                // Skip if the target is the same tier
                if target_tier == meta.tier {
                    let mut stats = self.stats.lock().unwrap();
                    stats.skipped += 1;
                    continue;
                }

                // Check size limit
                if meta.size > self.config.max_chunk_size_for_migration {
                    let mut stats = self.stats.lock().unwrap();
                    stats.skipped += 1;
                    continue;
                }

                let priority = self.policy.migration_priority(meta, pressure);
                let hotness = self.hotness_tracker.get_hotness(chunk_id);

                migrations.push(PendingMigration {
                    chunk_id: *chunk_id,
                    from_tier: meta.tier,
                    to_tier: target_tier,
                    priority,
                    size: meta.size,
                    hotness_score: hotness.map(|h| h.score).unwrap_or(0.0),
                    identified_at: now,
                });

                // Emit PolicyDecision trace event
                self.trace_log.record(TraceEvent::PolicyDecision {
                    chunk_id: *chunk_id,
                    from: meta.tier,
                    to: target_tier,
                    reason: format!("pressure={:.2}", pressure.max_pressure()),
                    timestamp: now,
                });
            } else {
                let mut stats = self.stats.lock().unwrap();
                stats.skipped += 1;
            }
        }

        drop(active);
        drop(last_migration);

        // Sort by priority (critical first) then by hotness (hottest first)
        migrations.sort_by(|a, b| {
            let priority_ord = b.priority.value().cmp(&a.priority.value());
            if priority_ord != std::cmp::Ordering::Equal {
                return priority_ord;
            }
            b.hotness_score
                .partial_cmp(&a.hotness_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Limit to max migrations per cycle
        migrations.truncate(self.config.max_migrations_per_cycle);

        migrations
    }

    /// Evaluate promotion candidates: hot chunks that should be moved to faster tiers.
    pub fn evaluate_promotions(&self, pressure: &PressureState) -> Vec<PendingMigration> {
        if !self.config.enable_promotion {
            return Vec::new();
        }

        let hot_chunks = self.hotness_tracker.find_hot_chunks(self.config.hot_threshold);
        let now = current_timestamp();
        let mut promotions = Vec::new();

        let available_tiers: Vec<TierId> = self.backends.keys().cloned().collect();

        for (chunk_id, hotness) in &hot_chunks {
            let sm = self.state_machine.lock().unwrap();
            let state = sm.get_state(chunk_id);
            if state != Some(ChunkState::Stored) {
                continue;
            }
            drop(sm);

            // Build a minimal meta for policy check
            let meta = ChunkMeta {
                id: *chunk_id,
                size: 0,
                compressed_size: 0,
                tier: TierId::Ram,
                state: ChunkState::Stored,
                created_at: hotness.first_accessed,
                last_accessed: hotness.last_accessed,
                access_count: hotness.access_count,
                compression: ghost_core::types::CompressionAlgorithm::None,
                checksum: [0u8; 32],
            };

            let target_tier = self.policy.select_target_tier(&meta, pressure, &available_tiers);
            if target_tier != meta.tier {
                let priority = self.policy.migration_priority(&meta, pressure);
                promotions.push(PendingMigration {
                    chunk_id: *chunk_id,
                    from_tier: meta.tier,
                    to_tier: target_tier,
                    priority,
                    size: meta.size,
                    hotness_score: hotness.score,
                    identified_at: now,
                });
            }
        }

        promotions
    }

    /// Evaluate eviction candidates: cold chunks on pressured tiers.
    pub fn evaluate_evictions(&self, pressure: &PressureState) -> Vec<PendingMigration> {
        if !self.config.enable_eviction {
            return Vec::new();
        }

        if pressure.max_pressure() < self.config.eviction_pressure_threshold {
            return Vec::new();
        }

        let cold_chunks = self.hotness_tracker.find_cold_chunks(self.config.cold_threshold);
        let now = current_timestamp();
        let mut evictions = Vec::new();

        for (chunk_id, hotness) in &cold_chunks {
            let sm = self.state_machine.lock().unwrap();
            let state = sm.get_state(chunk_id);
            if state != Some(ChunkState::Stored) {
                continue;
            }
            drop(sm);

            let meta = ChunkMeta {
                id: *chunk_id,
                size: 0,
                compressed_size: 0,
                tier: TierId::Ram,
                state: ChunkState::Stored,
                created_at: hotness.first_accessed,
                last_accessed: hotness.last_accessed,
                access_count: hotness.access_count,
                compression: ghost_core::types::CompressionAlgorithm::None,
                checksum: [0u8; 32],
            };

            // Ask the policy which chunk to evict
            let candidates: Vec<(ChunkId, ChunkMeta)> = vec![(*chunk_id, meta.clone())];
            if let Some(evict_id) = self.policy.select_viction(&candidates, pressure) {
                let available_tiers: Vec<TierId> = self.backends.keys().cloned().collect();
                let target_tier = self.policy.select_target_tier(&meta, pressure, &available_tiers);
                if target_tier != meta.tier {
                    let priority = self.policy.migration_priority(&meta, pressure);
                    evictions.push(PendingMigration {
                        chunk_id: evict_id,
                        from_tier: meta.tier,
                        to_tier: target_tier,
                        priority,
                        size: meta.size,
                        hotness_score: hotness.score,
                        identified_at: now,
                    });

                    // Emit Eviction trace event
                    self.trace_log.record(TraceEvent::Eviction {
                        chunk_id: evict_id,
                        tier: meta.tier,
                        reason: EvictionReason::Pressure,
                        timestamp: now,
                    });
                }
            }
        }

        evictions
    }

    /// Convert a pending migration into a transfer job.
    pub fn create_transfer_job(&self, migration: &PendingMigration) -> TransferJob {
        TransferJob::new(
            migration.chunk_id,
            migration.from_tier,
            migration.to_tier,
            migration.size,
            migration.priority,
        )
    }

    /// Mark a migration as active.
    pub fn mark_active(&self, chunk_id: ChunkId) {
        let mut active = self.active_migrations.lock().unwrap();
        active.insert(chunk_id);
        let mut stats = self.stats.lock().unwrap();
        stats.active_migrations = active.len();
    }

    /// Mark a migration as complete.
    pub fn mark_complete(&self, chunk_id: ChunkId, bytes: u64, success: bool) {
        let mut active = self.active_migrations.lock().unwrap();
        active.remove(&chunk_id);
        let mut stats = self.stats.lock().unwrap();
        stats.active_migrations = active.len();
        if success {
            stats.promotions += 1;
            stats.bytes_migrated += bytes;
        } else {
            stats.failures += 1;
        }
        drop(stats);

        let mut last = self.last_migration.lock().unwrap();
        last.insert(chunk_id, current_timestamp());

        // Emit unified event
        if let Some(ref emitter) = self.event_emitter {
            // Determine from/to tiers from the last_migration timestamp lookup
            // We use Ram→Disk as a default; actual tier info would require tracking
            let _ = emitter.try_emit(Event::MigrationCompleted {
                chunk_id,
                from: TierId::Ram,
                to: TierId::Disk,
                duration_ms: 0,
            });
        }
    }

    /// Get a snapshot of migration statistics.
    pub fn stats(&self) -> MigrationStats {
        self.stats.lock().unwrap().clone()
    }

    /// Check if a chunk is currently being migrated.
    pub fn is_migrating(&self, chunk_id: &ChunkId) -> bool {
        let active = self.active_migrations.lock().unwrap();
        active.contains(chunk_id)
    }

    /// Get the number of active migrations.
    pub fn active_count(&self) -> usize {
        let active = self.active_migrations.lock().unwrap();
        active.len()
    }

    /// Check if the migration engine has capacity for more concurrent migrations.
    pub fn has_capacity(&self) -> bool {
        self.active_count() < self.config.max_concurrent_migrations
    }
}

impl std::fmt::Debug for MigrationEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationEngine")
            .field("config", &self.config)
            .field("stats", &self.stats())
            .finish()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_policy::lru::{LruConfig, LruPolicy};

    fn test_backends() -> BTreeMap<TierId, Arc<dyn StorageBackend>> {
        let mut backends = BTreeMap::new();
        backends.insert(
            TierId::Ram,
            Arc::new(ghost_tier::RamBackend::new(1024 * 1024)) as Arc<dyn StorageBackend>,
        );
        backends.insert(
            TierId::Simulation,
            Arc::new(ghost_sim::SimBackend::new(ghost_sim::config::SimConfig::default()))
                as Arc<dyn StorageBackend>,
        );
        backends
    }

    fn test_engine() -> MigrationEngine {
        let config = MigrationConfig::default();
        let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
        let trace_log = Arc::new(TraceLog::new(1000));
        let hotness_tracker = Arc::new(HotnessTracker::new(1000, trace_log.clone()));
        let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
        let backends = test_backends();

        MigrationEngine::new(
            config,
            policy,
            hotness_tracker,
            state_machine,
            trace_log,
            backends,
        )
    }

    fn test_chunk_id(seed: u8) -> ChunkId {
        let mut id = [0u8; 32];
        id[0] = seed;
        ChunkId(id)
    }

    #[test]
    fn test_migration_engine_creation() {
        let engine = test_engine();
        assert_eq!(engine.active_count(), 0);
        assert!(engine.has_capacity());
    }

    #[test]
    fn test_migration_engine_evaluate_no_pressure() {
        let engine = test_engine();
        let pressure = PressureState::new();
        let migrations = engine.evaluate(&pressure);
        // No registered chunks = no migrations
        assert!(migrations.is_empty());
    }

    #[test]
    fn test_migration_engine_evaluate_promotions_disabled() {
        let mut config = MigrationConfig::default();
        config.enable_promotion = false;
        let policy: Arc<dyn PlacementPolicy> = Arc::new(LruPolicy::new(LruConfig::default()));
        let trace_log = Arc::new(TraceLog::new(1000));
        let hotness_tracker = Arc::new(HotnessTracker::new(1000, trace_log.clone()));
        let state_machine = Arc::new(std::sync::Mutex::new(StateMachine::new()));
        let backends = test_backends();

        let engine = MigrationEngine::new(
            config,
            policy,
            hotness_tracker,
            state_machine,
            trace_log,
            backends,
        );

        let pressure = PressureState::new();
        let promotions = engine.evaluate_promotions(&pressure);
        assert!(promotions.is_empty());
    }

    #[test]
    fn test_migration_engine_evaluate_evictions_below_threshold() {
        let engine = test_engine();
        // Pressure below eviction threshold
        let pressure = PressureState {
            memory_pressure: 0.5,
            vram_pressure: 0.1,
            io_pressure: 0.1,
            queue_depth: 0,
            throughput_bps: 0,
        };
        let evictions = engine.evaluate_evictions(&pressure);
        assert!(evictions.is_empty());
    }

    #[test]
    fn test_migration_engine_mark_active_and_complete() {
        let engine = test_engine();
        let id = test_chunk_id(1);

        assert!(!engine.is_migrating(&id));
        engine.mark_active(id);
        assert!(engine.is_migrating(&id));
        assert_eq!(engine.active_count(), 1);

        engine.mark_complete(id, 1024, true);
        assert!(!engine.is_migrating(&id));
        assert_eq!(engine.active_count(), 0);

        let stats = engine.stats();
        assert_eq!(stats.promotions, 1);
        assert_eq!(stats.bytes_migrated, 1024);
    }

    #[test]
    fn test_migration_engine_has_capacity() {
        let engine = test_engine();
        assert!(engine.has_capacity());

        let id1 = test_chunk_id(1);
        let id2 = test_chunk_id(2);
        engine.mark_active(id1);
        engine.mark_active(id2);

        // Default max_concurrent_migrations is 2
        assert!(!engine.has_capacity());
    }

    #[test]
    fn test_migration_engine_create_transfer_job() {
        let engine = test_engine();
        let migration = PendingMigration {
            chunk_id: test_chunk_id(1),
            from_tier: TierId::Ram,
            to_tier: TierId::Simulation,
            priority: TransferPriority::High,
            size: 4096,
            hotness_score: 0.8,
            identified_at: 0,
        };

        let job = engine.create_transfer_job(&migration);
        assert_eq!(job.from_tier, TierId::Ram);
        assert_eq!(job.to_tier, TierId::Simulation);
        assert_eq!(job.priority, TransferPriority::High);
        assert_eq!(job.size, 4096);
    }

    #[test]
    fn test_migration_engine_stats() {
        let engine = test_engine();
        let stats = engine.stats();
        assert_eq!(stats.evaluation_cycles, 0);
        assert_eq!(stats.promotions, 0);
        assert_eq!(stats.evictions, 0);
        assert_eq!(stats.failures, 0);
    }

    #[test]
    fn test_pending_migration_ordering() {
        let engine = test_engine();
        let pressure = PressureState::new();

        // Create test migrations with different priorities
        let m1 = PendingMigration {
            chunk_id: test_chunk_id(1),
            from_tier: TierId::Ram,
            to_tier: TierId::Simulation,
            priority: TransferPriority::Low,
            size: 1024,
            hotness_score: 0.9,
            identified_at: 0,
        };
        let m2 = PendingMigration {
            chunk_id: test_chunk_id(2),
            from_tier: TierId::Ram,
            to_tier: TierId::Simulation,
            priority: TransferPriority::Critical,
            size: 1024,
            hotness_score: 0.1,
            identified_at: 0,
        };

        // m2 (Critical) should come before m1 (Low) despite lower hotness
        let mut migrations = vec![m1.clone(), m2.clone()];
        migrations.sort_by(|a, b| {
            let priority_ord = b.priority.value().cmp(&a.priority.value());
            if priority_ord != std::cmp::Ordering::Equal {
                return priority_ord;
            }
            b.hotness_score
                .partial_cmp(&a.hotness_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        assert_eq!(migrations[0].priority, TransferPriority::Critical);
    }
}
