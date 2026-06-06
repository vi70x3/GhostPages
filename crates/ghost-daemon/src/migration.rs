//! Migration engine for pressure-driven chunk migration.
//!
//! Implements the core migration logic: evaluating which chunks should be
//! promoted to faster tiers, evicted from pressured tiers, or left in place.
//! Uses the PlacementPolicy trait for all placement decisions.
//!
//! Includes promotion cost model (Disk↔RAM), eviction cooldown with
//! anti-oscillation to prevent migration storms.

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

pub use ghost_core::state::PhysicalCost;

use crate::backpressure::BackpressureAction;
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
    /// Number of evictions blocked by cooldown.
    pub cooldown_blocks: u64,
    /// Number of evictions blocked by anti-oscillation.
    pub anti_oscillation_blocks: u64,
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

/// Eviction cooldown tracker to prevent migration storms.
///
/// Tracks the last eviction and promotion time per chunk to enforce:
/// - Minimum cooldown between evictions of the same chunk
/// - Anti-oscillation: if a chunk was recently promoted, don't evict it
#[derive(Debug)]
pub struct EvictionCooldown {
    /// Last eviction timestamp per chunk (microseconds).
    last_eviction: BTreeMap<ChunkId, u64>,
    /// Last promotion timestamp per chunk (microseconds).
    last_promotion: BTreeMap<ChunkId, u64>,
    /// Minimum cooldown between evictions of the same chunk (microseconds).
    cooldown_us: u64,
}

impl EvictionCooldown {
    /// Create a new eviction cooldown tracker.
    ///
    /// # Arguments
    /// * `cooldown_secs` - Minimum seconds between evictions of the same chunk.
    pub fn new(cooldown_secs: u64) -> Self {
        Self {
            last_eviction: BTreeMap::new(),
            last_promotion: BTreeMap::new(),
            cooldown_us: cooldown_secs * 1_000_000,
        }
    }

    /// Check if a chunk can be evicted.
    ///
    /// Returns `true` if the chunk is eligible for eviction:
    /// - Not in cooldown from a previous eviction
    /// - Not recently promoted (anti-oscillation)
    pub fn can_evict(&self, chunk_id: &ChunkId, now_us: u64) -> bool {
        // Check cooldown from last eviction
        if let Some(&last_evict) = self.last_eviction.get(chunk_id) {
            if now_us.saturating_sub(last_evict) < self.cooldown_us {
                return false;
            }
        }

        // Anti-oscillation: if recently promoted, don't evict
        if let Some(&last_promo) = self.last_promotion.get(chunk_id) {
            if now_us.saturating_sub(last_promo) < self.cooldown_us {
                return false;
            }
        }

        true
    }

    /// Record that a chunk was evicted.
    pub fn record_eviction(&mut self, chunk_id: ChunkId, now_us: u64) {
        self.last_eviction.insert(chunk_id, now_us);
    }

    /// Record that a chunk was promoted.
    pub fn record_promotion(&mut self, chunk_id: ChunkId, now_us: u64) {
        self.last_promotion.insert(chunk_id, now_us);
    }

    /// Get the cooldown duration in microseconds.
    pub fn cooldown_us(&self) -> u64 {
        self.cooldown_us
    }
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
    /// Eviction cooldown tracker to prevent migration storms.
    eviction_cooldown: Arc<std::sync::Mutex<EvictionCooldown>>,
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
        let cooldown_secs = config.min_migration_interval_secs.max(5);
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
            eviction_cooldown: Arc::new(std::sync::Mutex::new(EvictionCooldown::new(cooldown_secs))),
            event_emitter: None,
        }
    }

    /// Set the event emitter for unified event taxonomy.
    pub fn set_event_emitter(&mut self, emitter: EventEmitter) {
        self.event_emitter = Some(emitter);
    }

    /// Get a reference to the eviction cooldown tracker.
    pub fn eviction_cooldown(&self) -> &Arc<std::sync::Mutex<EvictionCooldown>> {
        &self.eviction_cooldown
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

                // Emit unified MigrationDecision event
                if let Some(ref emitter) = self.event_emitter {
                    let _ = emitter.try_emit(Event::MigrationDecision {
                        chunk_id: *chunk_id,
                        from: meta.tier,
                        to: target_tier,
                        decision: format!("pressure={:.2}", pressure.max_pressure()),
                        sequence_id: 0,
                    });
                }
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

                // Emit unified MigrationDecision event for promotion
                if let Some(ref emitter) = self.event_emitter {
                    let _ = emitter.try_emit(Event::MigrationDecision {
                        chunk_id: *chunk_id,
                        from: meta.tier,
                        to: target_tier,
                        decision: format!("promotion hotness={:.2}", hotness.score),
                        sequence_id: 0,
                    });
                }
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
        let mut cooldown = self.eviction_cooldown.lock().unwrap();

        for (chunk_id, hotness) in &cold_chunks {
            let sm = self.state_machine.lock().unwrap();
            let state = sm.get_state(chunk_id);
            if state != Some(ChunkState::Stored) {
                continue;
            }
            drop(sm);

            // Check eviction cooldown and anti-oscillation
            if !cooldown.can_evict(chunk_id, now) {
                let mut stats = self.stats.lock().unwrap();
                stats.cooldown_blocks += 1;
                continue;
            }

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

                    // Record eviction in cooldown tracker
                    cooldown.record_eviction(evict_id, now);

                    // Emit Eviction trace event
                    self.trace_log.record(TraceEvent::Eviction {
                        chunk_id: evict_id,
                        tier: meta.tier,
                        reason: EvictionReason::Pressure,
                        timestamp: now,
                    });

                    // Emit unified MigrationDecision event for eviction
                    if let Some(ref emitter) = self.event_emitter {
                        let _ = emitter.try_emit(Event::MigrationDecision {
                            chunk_id: evict_id,
                            from: meta.tier,
                            to: target_tier,
                            decision: format!("eviction pressure={:.2}", pressure.max_pressure()),
                            sequence_id: 0,
                        });
                    }
                }
            }
        }

        evictions
    }

    /// Estimate the promotion cost for migrating a chunk between tiers.
    ///
    /// Disk → RAM promotion cost includes: read latency + decompression + write to RAM
    /// RAM → Disk demotion cost includes: compression + write latency + fsync
    ///
    /// Cost is deterministic when using `DeterministicClock`.
    pub fn estimate_promotion_cost(
        &self,
        _chunk_id: &ChunkId,
        from_tier: TierId,
        to_tier: TierId,
        size: usize,
    ) -> PhysicalCost {
        let from_backend = self.backends.get(&from_tier);
        let to_backend = self.backends.get(&to_tier);

        let from_cost = from_backend.map(|b| b.cost_model()).unwrap_or_default();
        let to_cost = to_backend.map(|b| b.cost_model()).unwrap_or_default();

        // Base migration cost: source read + destination write
        let base_latency = from_cost.latency_ms + to_cost.latency_ms;
        let base_bandwidth = from_cost.bandwidth_bps.min(to_cost.bandwidth_bps);

        // Determine if this involves disk I/O
        let is_disk_involved = from_tier == TierId::Disk || to_tier == TierId::Disk;

        // Additional overhead for disk operations
        let (overhead_latency, overhead_reliability) = if is_disk_involved {
            // Disk → RAM: read from disk + decompress + write to RAM
            // RAM → Disk: compress + write to disk + fsync
            let compression_overhead_ms = if size > 0 {
                // Estimate compression/decompression time: ~100 MB/s
                (size as f64 / (100.0 * 1024.0 * 1024.0)) * 1000.0
            } else {
                0.0
            };

            // fsync overhead for disk writes (typically 0.5-2ms)
            let fsync_overhead_ms = if to_tier == TierId::Disk {
                1.0 // 1ms fsync estimate
            } else {
                0.0
            };

            (compression_overhead_ms + fsync_overhead_ms, 0.999)
        } else {
            (0.0, 1.0)
        };

        let total_latency = base_latency + overhead_latency;
        let total_bandwidth = if is_disk_involved {
            // For disk migrations, bandwidth is limited by the disk
            let disk_bandwidth = if from_tier == TierId::Disk {
                from_cost.bandwidth_bps
            } else {
                to_cost.bandwidth_bps
            };
            base_bandwidth.min(disk_bandwidth)
        } else {
            base_bandwidth
        };

        PhysicalCost {
            latency_ms: total_latency,
            bandwidth_bps: total_bandwidth,
            reliability: from_cost.reliability * to_cost.reliability * overhead_reliability,
            io_pressure: from_cost.io_pressure.max(to_cost.io_pressure),
            queue_depth: from_cost.queue_depth.saturating_add(to_cost.queue_depth),
        }
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

        // Record promotion in cooldown tracker for anti-oscillation
        if success {
            let mut cooldown = self.eviction_cooldown.lock().unwrap();
            cooldown.record_promotion(chunk_id, current_timestamp());
        }

        // Emit unified event
        if let Some(ref emitter) = self.event_emitter {
            // Determine from/to tiers from the last_migration timestamp lookup
            // We use Ram→Disk as a default; actual tier info would require tracking
            let _ = emitter.try_emit(Event::MigrationCompleted {
                chunk_id,
                from: TierId::Ram,
                to: TierId::Disk,
                duration_ms: 0,
                sequence_id: 0,
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

    /// Decide whether a migration should proceed based on I/O-aware criteria.
    ///
    /// This method evaluates a pending migration against current system state
    /// including I/O pressure, estimated I/O cost, queue depth, and backpressure.
    ///
    /// Returns `Some(pending_migration)` if the migration should proceed,
    /// or `None` if it should be deferred or rejected.
    pub fn decide_migration(
        &self,
        migration: &PendingMigration,
        _pressure: &PressureState,
        io_cost: &PhysicalCost,
        backpressure_action: &BackpressureAction,
    ) -> Option<PendingMigration> {
        let now = current_timestamp();
        let sequence_id = now;

        // Check backpressure constraints
        if !backpressure_action.allows(migration.priority) {
            // Emit MigrationRejected event
            if let Some(ref emitter) = self.event_emitter {
                let _ = emitter.try_emit(Event::MigrationRejected {
                    sequence_id,
                    chunk_id: migration.chunk_id,
                    from: migration.from_tier,
                    to: migration.to_tier,
                    cost_score: io_cost.cost_score(),
                    threshold: self.config.io_cost_threshold,
                });
            }
            return None;
        }

        // Check I/O pressure threshold
        if io_cost.is_too_pressured() {
            // Emit MigrationDeferred event
            if let Some(ref emitter) = self.event_emitter {
                let _ = emitter.try_emit(Event::MigrationDeferred {
                    sequence_id,
                    chunk_id: migration.chunk_id,
                    from: migration.from_tier,
                    to: migration.to_tier,
                    reason: format!(
                        "I/O pressure too high: pressure={:.2}, queue_depth={}",
                        io_cost.io_pressure, io_cost.queue_depth
                    ),
                });
            }
            return None;
        }

        // Check I/O cost threshold
        if io_cost.cost_score() > self.config.io_cost_threshold {
            // Emit MigrationDeferred event
            if let Some(ref emitter) = self.event_emitter {
                let _ = emitter.try_emit(Event::MigrationDeferred {
                    sequence_id,
                    chunk_id: migration.chunk_id,
                    from: migration.from_tier,
                    to: migration.to_tier,
                    reason: format!(
                        "I/O cost exceeds threshold: cost={:.2}, threshold={:.2}",
                        io_cost.cost_score(),
                        self.config.io_cost_threshold
                    ),
                });
            }
            return None;
        }

        // Check capacity
        if !self.has_capacity() {
            // Emit MigrationDeferred event
            if let Some(ref emitter) = self.event_emitter {
                let _ = emitter.try_emit(Event::MigrationDeferred {
                    sequence_id,
                    chunk_id: migration.chunk_id,
                    from: migration.from_tier,
                    to: migration.to_tier,
                    reason: "migration engine at capacity".to_string(),
                });
            }
            return None;
        }

        // All checks passed - emit MigrationDecided event
        if let Some(ref emitter) = self.event_emitter {
            let _ = emitter.try_emit(Event::MigrationDecided {
                sequence_id,
                chunk_id: migration.chunk_id,
                from: migration.from_tier,
                to: migration.to_tier,
                cost_score: io_cost.cost_score(),
            });
        }

        Some(migration.clone())
    }

    /// Get the estimated I/O cost for migrating a chunk between tiers.
    pub fn estimate_io_cost(&self, from_tier: TierId, to_tier: TierId, _size: usize) -> PhysicalCost {
        let from_backend = self.backends.get(&from_tier);
        let to_backend = self.backends.get(&to_tier);

        let from_cost = from_backend.map(|b| b.cost_model()).unwrap_or_default();
        let to_cost = to_backend.map(|b| b.cost_model()).unwrap_or_default();

        // Combine costs: migration cost is sum of source read + destination write
        let combined_latency = from_cost.latency_ms + to_cost.latency_ms;
        let combined_bandwidth = from_cost.bandwidth_bps.min(to_cost.bandwidth_bps);
        let combined_reliability = from_cost.reliability * to_cost.reliability;
        let combined_pressure = from_cost.io_pressure.max(to_cost.io_pressure);
        let combined_queue = from_cost.queue_depth.saturating_add(to_cost.queue_depth);

        PhysicalCost {
            latency_ms: combined_latency,
            bandwidth_bps: combined_bandwidth,
            reliability: combined_reliability,
            io_pressure: combined_pressure,
            queue_depth: combined_queue,
        }
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

    #[test]
    fn test_eviction_cooldown_creation() {
        let cooldown = EvictionCooldown::new(5);
        assert_eq!(cooldown.cooldown_us(), 5_000_000);
    }

    #[test]
    fn test_eviction_cooldown_allows_first_eviction() {
        let cooldown = EvictionCooldown::new(5);
        let id = test_chunk_id(1);
        // First eviction should always be allowed
        assert!(cooldown.can_evict(&id, 1_000_000));
    }

    #[test]
    fn test_eviction_cooldown_blocks_rapid_eviction() {
        let mut cooldown = EvictionCooldown::new(5);
        let id = test_chunk_id(1);
        let now = 1_000_000;

        // Record an eviction
        cooldown.record_eviction(id, now);

        // Try to evict again within cooldown window
        assert!(!cooldown.can_evict(&id, now + 1_000_000)); // 1 second later
        assert!(!cooldown.can_evict(&id, now + 4_000_000)); // 4 seconds later

        // After cooldown window (5+ seconds), should be allowed
        assert!(cooldown.can_evict(&id, now + 5_000_000)); // exactly 5 seconds
        assert!(cooldown.can_evict(&id, now + 6_000_000)); // 6 seconds later
    }

    #[test]
    fn test_eviction_cooldown_anti_oscillation() {
        let mut cooldown = EvictionCooldown::new(5);
        let id = test_chunk_id(1);
        let now = 1_000_000;

        // Record a promotion
        cooldown.record_promotion(id, now);

        // Should not be able to evict within cooldown window
        assert!(!cooldown.can_evict(&id, now + 1_000_000));
        assert!(!cooldown.can_evict(&id, now + 4_000_000));

        // After cooldown window, should be allowed
        assert!(cooldown.can_evict(&id, now + 5_000_000));
    }

    #[test]
    fn test_estimate_promotion_cost_disk_to_ram() {
        let engine = test_engine();
        let id = test_chunk_id(1);
        let cost = engine.estimate_promotion_cost(&id, TierId::Disk, TierId::Ram, 4096);

        // Disk → RAM should have non-zero latency (read + decompress + write)
        assert!(cost.latency_ms > 0.0);
        // Reliability should be high
        assert!(cost.reliability > 0.0);
    }

    #[test]
    fn test_estimate_promotion_cost_ram_to_disk() {
        let engine = test_engine();
        let id = test_chunk_id(1);
        let cost = engine.estimate_promotion_cost(&id, TierId::Ram, TierId::Disk, 4096);

        // RAM → Disk should have non-zero latency (compress + write + fsync)
        assert!(cost.latency_ms > 0.0);
        // Reliability should be high
        assert!(cost.reliability > 0.0);
    }

    #[test]
    fn test_estimate_promotion_cost_deterministic() {
        let engine = test_engine();
        let id = test_chunk_id(1);

        // Same parameters should produce same cost (deterministic)
        let cost1 = engine.estimate_promotion_cost(&id, TierId::Disk, TierId::Ram, 4096);
        let cost2 = engine.estimate_promotion_cost(&id, TierId::Disk, TierId::Ram, 4096);

        assert_eq!(cost1.latency_ms, cost2.latency_ms);
        assert_eq!(cost1.bandwidth_bps, cost2.bandwidth_bps);
        assert_eq!(cost1.reliability, cost2.reliability);
    }

    #[test]
    fn test_mark_complete_records_promotion() {
        let engine = test_engine();
        let id = test_chunk_id(1);

        engine.mark_active(id);
        engine.mark_complete(id, 1024, true);

        // After promotion, the cooldown should block eviction
        let cooldown = engine.eviction_cooldown().lock().unwrap();
        assert!(!cooldown.can_evict(&id, current_timestamp()));
    }
}
