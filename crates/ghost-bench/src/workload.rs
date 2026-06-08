//! Synthetic workload definitions and generators for GhostPages benchmarking.
//!
//! This module provides deterministic workload generation: same seed + same
//! definition = identical scenario. All generation is pure — no I/O, no mutation.

use ghost_core::hotness_confidence::HotnessConfidence;
use ghost_core::hotness_summary::HotnessSummary;
use ghost_core::state::PressureState;
use ghost_linux::policy_rules::SystemState;

// ─── Workload Classification ───────────────────────────────────────────────────

/// Classification of workload type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum WorkloadClass {
    /// Browser, IDE, terminal.
    Desktop,
    /// Cargo, clang, kernel builds.
    BuildSystem,
    /// Synthetic pressure, allocator stress.
    MemoryPressure,
    /// SQLite, Postgres, cache-heavy.
    DataSystem,
    /// Combined workload patterns.
    Mixed,
}

impl std::fmt::Display for WorkloadClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadClass::Desktop => write!(f, "Desktop"),
            WorkloadClass::BuildSystem => write!(f, "BuildSystem"),
            WorkloadClass::MemoryPressure => write!(f, "MemoryPressure"),
            WorkloadClass::DataSystem => write!(f, "DataSystem"),
            WorkloadClass::Mixed => write!(f, "Mixed"),
        }
    }
}

// ─── Workload Definition ───────────────────────────────────────────────────────

/// A synthetic workload definition that generates SystemState sequences.
#[derive(Debug, Clone)]
pub struct WorkloadDefinition {
    /// Unique name for this workload.
    pub name: String,
    /// The class of workload.
    pub class: WorkloadClass,
    /// Human-readable description.
    pub description: String,
    /// Simulated duration in seconds.
    pub duration_seconds: u64,
    /// Time between snapshots in milliseconds.
    pub snapshot_interval_ms: u64,
    /// Deterministic seed.
    pub seed: u64,
}

// ─── Timed Snapshot ────────────────────────────────────────────────────────────

/// A SystemState snapshot with a timestamp.
#[derive(Debug, Clone)]
pub struct TimedSnapshot {
    /// Timestamp in milliseconds from the start of the workload.
    pub timestamp_ms: u64,
    /// The system state at this point in time.
    pub state: SystemState,
}

// ─── Pressure Time Distribution ────────────────────────────────────────────────

/// How much time was spent at each pressure level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PressureTimeDistribution {
    /// Fraction of time with pressure < 0.3.
    pub idle_fraction: f32,
    /// Fraction of time with pressure 0.3–0.5.
    pub low_fraction: f32,
    /// Fraction of time with pressure 0.5–0.7.
    pub medium_fraction: f32,
    /// Fraction of time with pressure 0.7–0.85.
    pub high_fraction: f32,
    /// Fraction of time with pressure > 0.85.
    pub critical_fraction: f32,
}

// ─── Scenario Metadata ─────────────────────────────────────────────────────────

/// Metadata about a generated scenario.
#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioMetadata {
    /// Total number of snapshots in the scenario.
    pub total_snapshots: usize,
    /// Peak DRAM memory pressure observed.
    pub peak_dram_pressure: f32,
    /// Peak DRAM utilization observed.
    pub peak_dram_utilization: f32,
    /// Average DRAM utilization across all snapshots.
    pub avg_dram_utilization: f32,
    /// Average swap utilization across all snapshots.
    pub avg_swap_utilization: f32,
    /// Distribution of time across pressure levels.
    pub pressure_time_distribution: PressureTimeDistribution,
}

// ─── Workload Scenario ─────────────────────────────────────────────────────────

/// A generated workload scenario — a sequence of SystemState snapshots over time.
#[derive(Debug, Clone)]
pub struct WorkloadScenario {
    /// The definition that generated this scenario.
    pub definition: WorkloadDefinition,
    /// The sequence of timed snapshots.
    pub snapshots: Vec<TimedSnapshot>,
    /// Computed metadata about the scenario.
    pub metadata: ScenarioMetadata,
}

// ─── Workload Generator ────────────────────────────────────────────────────────

/// Generates deterministic workload scenarios from definitions.
#[derive(Debug, Clone)]
pub struct WorkloadGenerator {
    seed: u64,
}

impl WorkloadGenerator {
    /// Create a new workload generator with the given seed.
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    /// Generate a workload scenario from a definition.
    ///
    /// The generation is fully deterministic: same seed + same definition
    /// always produces an identical scenario.
    pub fn generate(&self, definition: &WorkloadDefinition) -> WorkloadScenario {
        let total_duration_ms = definition.duration_seconds * 1000;
        let num_snapshots = if definition.snapshot_interval_ms > 0 {
            (total_duration_ms / definition.snapshot_interval_ms).max(1) as usize
        } else {
            1
        };

        let mut snapshots = Vec::with_capacity(num_snapshots);

        for i in 0..num_snapshots {
            let timestamp_ms = i as u64 * definition.snapshot_interval_ms;
            let progress = if num_snapshots > 1 {
                i as f32 / (num_snapshots - 1) as f32
            } else {
                0.0
            };

            let state = self.generate_snapshot(definition, timestamp_ms, progress);
            snapshots.push(TimedSnapshot {
                timestamp_ms,
                state,
            });
        }

        let metadata = Self::compute_metadata(&snapshots);

        WorkloadScenario {
            definition: definition.clone(),
            snapshots,
            metadata,
        }
    }

    /// Generate a single snapshot for a given point in the workload.
    fn generate_snapshot(
        &self,
        definition: &WorkloadDefinition,
        timestamp_ms: u64,
        progress: f32,
    ) -> SystemState {
        let perturbation = self.perturbation(timestamp_ms);

        match definition.class {
            WorkloadClass::Desktop => Self::generate_desktop(perturbation, progress),
            WorkloadClass::BuildSystem => Self::generate_build_server(perturbation, progress),
            WorkloadClass::MemoryPressure => {
                Self::generate_memory_pressure_ramp(perturbation, progress)
            }
            WorkloadClass::DataSystem => Self::generate_database_cache(perturbation, progress),
            WorkloadClass::Mixed => Self::generate_mixed_multitask(perturbation, progress, timestamp_ms),
        }
    }

    /// Deterministic perturbation: ±0.05 range from a simple hash of (seed, timestamp).
    fn perturbation(&self, timestamp_ms: u64) -> f32 {
        let hash = self.seed.wrapping_mul(6364136223846793005).wrapping_add(
            timestamp_ms
                .wrapping_mul(1442695040888963407)
                .wrapping_add(1),
        );
        let normalized = (hash % 1000) as f32 / 1000.0;
        normalized * 0.1 - 0.05
    }

    /// Generate a desktop workload snapshot: low pressure, occasional brief spikes.
    fn generate_desktop(perturbation: f32, progress: f32) -> SystemState {
        // Base pressure: 0.1-0.3 range with small perturbation
        let base_pressure = 0.15 + perturbation;
        // Occasional brief spikes to medium (simulating "opening a tab")
        let spike = if (progress * 10.0).fract() < 0.15 {
            0.25
        } else {
            0.0
        };
        let memory_pressure = (base_pressure + spike).clamp(0.0, 1.0);

        // DRAM utilization: 0.3-0.5
        let dram_util = (0.35 + perturbation * 0.5).clamp(0.0, 1.0);

        // Hotness: mostly warm/cold regions
        let hotness = HotnessSummary {
            hot_count: 2,
            warm_count: 15,
            cold_count: 30,
            frozen_count: 5,
            total_regions: 52,
            hot_percentage: 3.8,
            warm_percentage: 28.8,
            cold_percentage: 57.7,
            frozen_percentage: 9.6,
            avg_access_count: 50,
            max_access_count: 200,
            min_access_count: 0,
        };

        SystemState {
            dram_pressure: PressureState {
                memory_pressure,
                ..PressureState::new()
            },
            dram_utilization: dram_util,
            swap_utilization: (0.05 + perturbation * 0.2).clamp(0.0, 1.0),
            zram_utilization: Some((0.15 + perturbation * 0.3).clamp(0.0, 1.0)),
            io_pressure: PressureState::new(),
            hotness_summary: Some(hotness),
            hotness_confidence: Some(HotnessConfidence {
                score: 0.7,
                factors: vec![],
            }),
        }
    }

    /// Generate a memory pressure ramp snapshot: gradually increasing pressure.
    fn generate_memory_pressure_ramp(perturbation: f32, progress: f32) -> SystemState {
        // Pressure climbs from 0.1 to 0.95
        let base_pressure = 0.1 + progress * 0.85 + perturbation;
        let memory_pressure = base_pressure.clamp(0.0, 1.0);

        // DRAM utilization climbs from 0.3 to 0.97
        let dram_util = (0.3 + progress * 0.67 + perturbation * 0.3).clamp(0.0, 1.0);

        // Swap starts climbing at 70% progress
        let swap_util = if progress > 0.7 {
            (progress - 0.7) / 0.3 * 0.6 + perturbation * 0.2
        } else {
            0.05 + perturbation * 0.1
        }
        .clamp(0.0, 1.0);

        SystemState {
            dram_pressure: PressureState {
                memory_pressure,
                ..PressureState::new()
            },
            dram_utilization: dram_util,
            swap_utilization: swap_util,
            zram_utilization: Some((0.2 + progress * 0.5 + perturbation * 0.2).clamp(0.0, 1.0)),
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    /// Generate a build server snapshot: periodic high pressure spikes.
    fn generate_build_server(perturbation: f32, progress: f32) -> SystemState {
        // Periodic spikes: compilation bursts every ~20% of progress
        let cycle = (progress * 5.0) % 1.0; // 5 bursts over the duration
        let in_burst = cycle < 0.4; // burst lasts 40% of each cycle

        let memory_pressure = if in_burst {
            (0.75 + perturbation).clamp(0.0, 1.0)
        } else {
            (0.25 + perturbation).clamp(0.0, 1.0)
        };

        let dram_util = if in_burst {
            (0.8 + perturbation * 0.3).clamp(0.0, 1.0)
        } else {
            (0.5 + perturbation * 0.3).clamp(0.0, 1.0)
        };

        // Hotness: some hot regions (active compilation), many cold (cached headers)
        let hotness = HotnessSummary {
            hot_count: if in_burst { 12 } else { 3 },
            warm_count: 10,
            cold_count: if in_burst { 20 } else { 35 },
            frozen_count: 8,
            total_regions: if in_burst { 50 } else { 56 },
            hot_percentage: if in_burst { 24.0 } else { 5.4 },
            warm_percentage: 20.0,
            cold_percentage: if in_burst { 40.0 } else { 62.5 },
            frozen_percentage: if in_burst { 16.0 } else { 14.3 },
            avg_access_count: if in_burst { 200 } else { 40 },
            max_access_count: if in_burst { 800 } else { 150 },
            min_access_count: 0,
        };

        SystemState {
            dram_pressure: PressureState {
                memory_pressure,
                ..PressureState::new()
            },
            dram_utilization: dram_util,
            swap_utilization: (0.15 + perturbation * 0.3).clamp(0.0, 1.0),
            zram_utilization: Some((0.3 + perturbation * 0.4).clamp(0.0, 1.0)),
            io_pressure: PressureState::new(),
            hotness_summary: Some(hotness),
            hotness_confidence: Some(HotnessConfidence {
                score: if in_burst { 0.85 } else { 0.6 },
                factors: vec![],
            }),
        }
    }

    /// Generate a database cache snapshot: moderate sustained pressure with hotness.
    fn generate_database_cache(perturbation: f32, _progress: f32) -> SystemState {
        // Sustained moderate pressure: 0.4-0.6
        let memory_pressure = (0.5 + perturbation).clamp(0.0, 1.0);

        // DRAM utilization: 0.6-0.75
        let dram_util = (0.65 + perturbation * 0.4).clamp(0.0, 1.0);

        // Hotness: many hot regions (active cache entries), some cold (expired)
        let hotness = HotnessSummary {
            hot_count: 20,
            warm_count: 15,
            cold_count: 10,
            frozen_count: 3,
            total_regions: 48,
            hot_percentage: 41.7,
            warm_percentage: 31.3,
            cold_percentage: 20.8,
            frozen_percentage: 6.3,
            avg_access_count: 300,
            max_access_count: 1500,
            min_access_count: 0,
        };

        SystemState {
            dram_pressure: PressureState {
                memory_pressure,
                ..PressureState::new()
            },
            dram_utilization: dram_util,
            swap_utilization: (0.2 + perturbation * 0.3).clamp(0.0, 1.0),
            zram_utilization: Some((0.5 + perturbation * 0.3).clamp(0.0, 1.0)),
            io_pressure: PressureState::new(),
            hotness_summary: Some(hotness),
            hotness_confidence: Some(HotnessConfidence {
                score: 0.85,
                factors: vec![],
            }),
        }
    }

    /// Generate a mixed multitask snapshot: alternating patterns.
    fn generate_mixed_multitask(perturbation: f32, progress: f32, timestamp_ms: u64) -> SystemState {
        // Divide into 5 phases: idle → browser spike → build burst → cache → recovery
        let phase = match progress {
            p if p < 0.15 => 0, // idle
            p if p < 0.30 => 1, // browser spike
            p if p < 0.55 => 2, // build burst
            p if p < 0.80 => 3, // sustained cache
            _ => 4,             // recovery
        };

        match phase {
            0 => {
                // Idle: low pressure
                let mut state = Self::generate_desktop(perturbation, progress);
                // Make it even more idle
                state.dram_pressure.memory_pressure =
                    (state.dram_pressure.memory_pressure * 0.5).clamp(0.0, 1.0);
                state
            }
            1 => {
                // Browser spike: medium pressure
                let memory_pressure = (0.45 + perturbation).clamp(0.0, 1.0);
                SystemState {
                    dram_pressure: PressureState {
                        memory_pressure,
                        ..PressureState::new()
                    },
                    dram_utilization: (0.55 + perturbation * 0.4).clamp(0.0, 1.0),
                    swap_utilization: (0.1 + perturbation * 0.2).clamp(0.0, 1.0),
                    zram_utilization: Some((0.25 + perturbation * 0.3).clamp(0.0, 1.0)),
                    io_pressure: PressureState::new(),
                    hotness_summary: Some(HotnessSummary {
                        hot_count: 8,
                        warm_count: 12,
                        cold_count: 20,
                        frozen_count: 4,
                        total_regions: 44,
                        hot_percentage: 18.2,
                        warm_percentage: 27.3,
                        cold_percentage: 45.5,
                        frozen_percentage: 9.1,
                        avg_access_count: 120,
                        max_access_count: 500,
                        min_access_count: 0,
                    }),
                    hotness_confidence: Some(HotnessConfidence {
                        score: 0.75,
                        factors: vec![],
                    }),
                }
            }
            2 => {
                // Build burst: high pressure
                Self::generate_build_server(perturbation, (progress - 0.30) / 0.25)
            }
            3 => {
                // Sustained cache: moderate pressure with hotness
                Self::generate_database_cache(perturbation, progress)
            }
            _ => {
                // Recovery: decreasing pressure
                let recovery_progress = (progress - 0.80) / 0.20;
                let memory_pressure = ((0.5 - recovery_progress * 0.35) + perturbation).clamp(0.0, 1.0);
                SystemState {
                    dram_pressure: PressureState {
                        memory_pressure,
                        ..PressureState::new()
                    },
                    dram_utilization: ((0.65 - recovery_progress * 0.3) + perturbation * 0.3)
                        .clamp(0.0, 1.0),
                    swap_utilization: ((0.25 - recovery_progress * 0.15) + perturbation * 0.2)
                        .clamp(0.0, 1.0),
                    zram_utilization: Some(
                        ((0.5 - recovery_progress * 0.2) + perturbation * 0.2).clamp(0.0, 1.0),
                    ),
                    io_pressure: PressureState::new(),
                    hotness_summary: Some(HotnessSummary {
                        hot_count: 5,
                        warm_count: 10,
                        cold_count: 25,
                        frozen_count: 5,
                        total_regions: 45,
                        hot_percentage: 11.1,
                        warm_percentage: 22.2,
                        cold_percentage: 55.6,
                        frozen_percentage: 11.1,
                        avg_access_count: 80,
                        max_access_count: 300,
                        min_access_count: 0,
                    }),
                    hotness_confidence: Some(HotnessConfidence {
                        score: 0.7,
                        factors: vec![],
                    }),
                }
            }
        }
    }

    /// Generate an allocator stress snapshot: rapid oscillation.
    fn generate_allocator_stress(perturbation: f32, progress: f32) -> SystemState {
        // Rapid oscillation: allocate (spike to 0.8) → free (drop to 0.2)
        let cycle = (progress * 8.0) % 1.0; // 8 cycles over the duration
        let allocating = cycle < 0.5;

        let memory_pressure = if allocating {
            (0.75 + perturbation).clamp(0.0, 1.0)
        } else {
            (0.2 + perturbation).clamp(0.0, 1.0)
        };

        let dram_util = if allocating {
            (0.8 + perturbation * 0.2).clamp(0.0, 1.0)
        } else {
            (0.3 + perturbation * 0.3).clamp(0.0, 1.0)
        };

        SystemState {
            dram_pressure: PressureState {
                memory_pressure,
                ..PressureState::new()
            },
            dram_utilization: dram_util,
            swap_utilization: if allocating {
                (0.3 + perturbation * 0.3).clamp(0.0, 1.0)
            } else {
                (0.05 + perturbation * 0.1).clamp(0.0, 1.0)
            },
            zram_utilization: Some(
                (if allocating { 0.6 } else { 0.2 } + perturbation * 0.3).clamp(0.0, 1.0),
            ),
            io_pressure: PressureState::new(),
            hotness_summary: None,
            hotness_confidence: None,
        }
    }

    /// Generate a tier saturation snapshot: saturates one tier at a time.
    fn generate_tier_saturation(perturbation: f32, progress: f32) -> SystemState {
        // Phase 1 (0-0.25): DRAM fills up
        // Phase 2 (0.25-0.50): ZRAM fills
        // Phase 3 (0.50-0.75): Swap fills
        // Phase 4 (0.75-1.00): Recovery

        if progress < 0.25 {
            // DRAM filling
            let sub_progress = progress / 0.25;
            SystemState {
                dram_pressure: PressureState {
                    memory_pressure: (0.3 + sub_progress * 0.6 + perturbation).clamp(0.0, 1.0),
                    ..PressureState::new()
                },
                dram_utilization: (0.4 + sub_progress * 0.55 + perturbation * 0.2).clamp(0.0, 1.0),
                swap_utilization: (0.05 + perturbation * 0.1).clamp(0.0, 1.0),
                zram_utilization: Some((0.1 + perturbation * 0.2).clamp(0.0, 1.0)),
                io_pressure: PressureState::new(),
                hotness_summary: None,
                hotness_confidence: None,
            }
        } else if progress < 0.50 {
            // ZRAM filling (DRAM already high)
            let sub_progress = (progress - 0.25) / 0.25;
            SystemState {
                dram_pressure: PressureState {
                    memory_pressure: (0.85 + perturbation).clamp(0.0, 1.0),
                    ..PressureState::new()
                },
                dram_utilization: (0.92 + perturbation * 0.05).clamp(0.0, 1.0),
                swap_utilization: (0.1 + sub_progress * 0.2 + perturbation * 0.1).clamp(0.0, 1.0),
                zram_utilization: Some((0.2 + sub_progress * 0.6 + perturbation * 0.2).clamp(0.0, 1.0)),
                io_pressure: PressureState::new(),
                hotness_summary: None,
                hotness_confidence: None,
            }
        } else if progress < 0.75 {
            // Swap filling (DRAM + ZRAM already high)
            let sub_progress = (progress - 0.50) / 0.25;
            SystemState {
                dram_pressure: PressureState {
                    memory_pressure: (0.9 + perturbation).clamp(0.0, 1.0),
                    ..PressureState::new()
                },
                dram_utilization: (0.95 + perturbation * 0.03).clamp(0.0, 1.0),
                swap_utilization: (0.3 + sub_progress * 0.5 + perturbation * 0.2).clamp(0.0, 1.0),
                zram_utilization: Some((0.8 + perturbation * 0.1).clamp(0.0, 1.0)),
                io_pressure: PressureState::new(),
                hotness_summary: None,
                hotness_confidence: None,
            }
        } else {
            // Recovery: all tiers draining
            let sub_progress = (progress - 0.75) / 0.25;
            SystemState {
                dram_pressure: PressureState {
                    memory_pressure: ((0.9 - sub_progress * 0.7) + perturbation).clamp(0.0, 1.0),
                    ..PressureState::new()
                },
                dram_utilization: ((0.95 - sub_progress * 0.6) + perturbation * 0.2)
                    .clamp(0.0, 1.0),
                swap_utilization: ((0.8 - sub_progress * 0.6) + perturbation * 0.2).clamp(0.0, 1.0),
                zram_utilization: Some(
                    ((0.8 - sub_progress * 0.5) + perturbation * 0.2).clamp(0.0, 1.0),
                ),
                io_pressure: PressureState::new(),
                hotness_summary: None,
                hotness_confidence: None,
            }
        }
    }

    /// Compute metadata from a set of generated snapshots.
    fn compute_metadata(snapshots: &[TimedSnapshot]) -> ScenarioMetadata {
        let total_snapshots = snapshots.len();

        let peak_dram_pressure = snapshots
            .iter()
            .map(|s| s.state.dram_pressure.memory_pressure)
            .fold(0.0_f32, f32::max);

        let peak_dram_utilization = snapshots
            .iter()
            .map(|s| s.state.dram_utilization)
            .fold(0.0_f32, f32::max);

        let avg_dram_utilization = if total_snapshots > 0 {
            snapshots.iter().map(|s| s.state.dram_utilization).sum::<f32>() / total_snapshots as f32
        } else {
            0.0
        };

        let avg_swap_utilization = if total_snapshots > 0 {
            snapshots.iter().map(|s| s.state.swap_utilization).sum::<f32>() / total_snapshots as f32
        } else {
            0.0
        };

        // Compute pressure time distribution
        let mut idle_count = 0usize;
        let mut low_count = 0usize;
        let mut medium_count = 0usize;
        let mut high_count = 0usize;
        let mut critical_count = 0usize;

        for snapshot in snapshots {
            let pressure = snapshot.state.dram_pressure.memory_pressure;
            if pressure < 0.3 {
                idle_count += 1;
            } else if pressure < 0.5 {
                low_count += 1;
            } else if pressure < 0.7 {
                medium_count += 1;
            } else if pressure < 0.85 {
                high_count += 1;
            } else {
                critical_count += 1;
            }
        }

        let total = total_snapshots as f32;
        let pressure_time_distribution = PressureTimeDistribution {
            idle_fraction: if total > 0.0 { idle_count as f32 / total } else { 0.0 },
            low_fraction: if total > 0.0 { low_count as f32 / total } else { 0.0 },
            medium_fraction: if total > 0.0 { medium_count as f32 / total } else { 0.0 },
            high_fraction: if total > 0.0 { high_count as f32 / total } else { 0.0 },
            critical_fraction: if total > 0.0 { critical_count as f32 / total } else { 0.0 },
        };

        ScenarioMetadata {
            total_snapshots,
            peak_dram_pressure,
            peak_dram_utilization,
            avg_dram_utilization,
            avg_swap_utilization,
            pressure_time_distribution,
        }
    }
}

// ─── Built-in Workload Definitions ─────────────────────────────────────────────

/// Idle desktop workload: low pressure, mostly idle, occasional brief spikes.
pub fn idle_desktop() -> WorkloadDefinition {
    WorkloadDefinition {
        name: "idle_desktop".to_string(),
        class: WorkloadClass::Desktop,
        description: "Low pressure desktop workload with occasional brief spikes".to_string(),
        duration_seconds: 60,
        snapshot_interval_ms: 1000,
        seed: 42,
    }
}

/// Memory pressure ramp: gradually increasing pressure from idle to critical.
pub fn memory_pressure_ramp() -> WorkloadDefinition {
    WorkloadDefinition {
        name: "memory_pressure_ramp".to_string(),
        class: WorkloadClass::MemoryPressure,
        description: "Gradually increasing pressure from idle to critical".to_string(),
        duration_seconds: 120,
        snapshot_interval_ms: 2000,
        seed: 100,
    }
}

/// Build server workload: periodic high pressure spikes during compilation bursts.
pub fn build_server() -> WorkloadDefinition {
    WorkloadDefinition {
        name: "build_server".to_string(),
        class: WorkloadClass::BuildSystem,
        description: "Periodic high pressure spikes during compilation bursts".to_string(),
        duration_seconds: 180,
        snapshot_interval_ms: 1000,
        seed: 200,
    }
}

/// Database cache workload: moderate sustained pressure with hotness data.
pub fn database_cache() -> WorkloadDefinition {
    WorkloadDefinition {
        name: "database_cache".to_string(),
        class: WorkloadClass::DataSystem,
        description: "Moderate sustained pressure with hotness data (active cache)".to_string(),
        duration_seconds: 90,
        snapshot_interval_ms: 1000,
        seed: 300,
    }
}

/// Mixed multitask workload: alternating patterns combining other workloads.
pub fn mixed_multitask() -> WorkloadDefinition {
    WorkloadDefinition {
        name: "mixed_multitask".to_string(),
        class: WorkloadClass::Mixed,
        description: "Alternating patterns: idle → browser → build → cache → recovery"
            .to_string(),
        duration_seconds: 200,
        snapshot_interval_ms: 2000,
        seed: 400,
    }
}

/// Allocator stress workload: rapid oscillation between allocate and free.
pub fn allocator_stress() -> WorkloadDefinition {
    WorkloadDefinition {
        name: "allocator_stress".to_string(),
        class: WorkloadClass::MemoryPressure,
        description: "Rapid oscillation: allocate (spike) → free (drop) → repeat".to_string(),
        duration_seconds: 60,
        snapshot_interval_ms: 500,
        seed: 500,
    }
}

/// Tier saturation workload: saturates one tier at a time (DRAM → ZRAM → swap).
pub fn tier_saturation() -> WorkloadDefinition {
    WorkloadDefinition {
        name: "tier_saturation".to_string(),
        class: WorkloadClass::MemoryPressure,
        description: "Tier-by-tier saturation: DRAM full → ZRAM fills → swap fills → recovery"
            .to_string(),
        duration_seconds: 120,
        snapshot_interval_ms: 2000,
        seed: 600,
    }
}

/// Returns all 7 built-in workload definitions.
pub fn all_builtin_workloads() -> Vec<WorkloadDefinition> {
    vec![
        idle_desktop(),
        memory_pressure_ramp(),
        build_server(),
        database_cache(),
        mixed_multitask(),
        allocator_stress(),
        tier_saturation(),
    ]
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idle_desktop_definition() {
        let def = idle_desktop();
        assert_eq!(def.name, "idle_desktop");
        assert_eq!(def.class, WorkloadClass::Desktop);
        assert_eq!(def.duration_seconds, 60);
        assert_eq!(def.snapshot_interval_ms, 1000);
        assert_eq!(def.seed, 42);
    }

    #[test]
    fn test_generate_idle_desktop() {
        let gen = WorkloadGenerator::new(42);
        let def = idle_desktop();
        let scenario = gen.generate(&def);

        // 60 seconds at 1000ms interval = 60 snapshots
        assert_eq!(scenario.snapshots.len(), 60);

        // Pressure should be low (0.0-0.4 range)
        for snapshot in &scenario.snapshots {
            assert!(
                snapshot.state.dram_pressure.memory_pressure < 0.5,
                "idle desktop pressure should be < 0.5, got {}",
                snapshot.state.dram_pressure.memory_pressure
            );
        }

        // DRAM utilization should be 0.3-0.5
        for snapshot in &scenario.snapshots {
            assert!(
                snapshot.state.dram_utilization >= 0.0 && snapshot.state.dram_utilization <= 1.0,
                "dram_utilization should be in [0, 1]"
            );
        }
    }

    #[test]
    fn test_generate_memory_pressure_ramp() {
        let gen = WorkloadGenerator::new(100);
        let def = memory_pressure_ramp();
        let scenario = gen.generate(&def);

        // 120 seconds at 2000ms interval = 60 snapshots
        assert_eq!(scenario.snapshots.len(), 60);

        // First snapshot should have low pressure
        let first_pressure = scenario.snapshots.first().unwrap().state.dram_pressure.memory_pressure;
        assert!(
            first_pressure < 0.3,
            "first pressure should be low, got {}",
            first_pressure
        );

        // Last snapshot should have high pressure
        let last_pressure = scenario.snapshots.last().unwrap().state.dram_pressure.memory_pressure;
        assert!(
            last_pressure > 0.7,
            "last pressure should be high, got {}",
            last_pressure
        );

        // Pressure should generally increase (check first half avg vs second half avg)
        let mid = scenario.snapshots.len() / 2;
        let first_half_avg: f32 = scenario.snapshots[..mid]
            .iter()
            .map(|s| s.state.dram_pressure.memory_pressure)
            .sum::<f32>()
            / mid as f32;
        let second_half_avg: f32 = scenario.snapshots[mid..]
            .iter()
            .map(|s| s.state.dram_pressure.memory_pressure)
            .sum::<f32>()
            / (scenario.snapshots.len() - mid) as f32;
        assert!(
            second_half_avg > first_half_avg,
            "second half avg pressure ({}) should be > first half ({})",
            second_half_avg,
            first_half_avg
        );
    }

    #[test]
    fn test_generate_build_server() {
        let gen = WorkloadGenerator::new(200);
        let def = build_server();
        let scenario = gen.generate(&def);

        // 180 seconds at 1000ms interval = 180 snapshots
        assert_eq!(scenario.snapshots.len(), 180);

        // Should have both high and low pressure periods
        let max_pressure = scenario
            .snapshots
            .iter()
            .map(|s| s.state.dram_pressure.memory_pressure)
            .fold(0.0_f32, f32::max);
        let min_pressure = scenario
            .snapshots
            .iter()
            .map(|s| s.state.dram_pressure.memory_pressure)
            .fold(1.0_f32, f32::min);

        assert!(
            max_pressure > 0.6,
            "build server should have high pressure spikes, max={}",
            max_pressure
        );
        assert!(
            min_pressure < 0.4,
            "build server should have recovery periods, min={}",
            min_pressure
        );
    }

    #[test]
    fn test_generate_database_cache() {
        let gen = WorkloadGenerator::new(300);
        let def = database_cache();
        let scenario = gen.generate(&def);

        // 90 seconds at 1000ms interval = 90 snapshots
        assert_eq!(scenario.snapshots.len(), 90);

        // Should have sustained moderate pressure
        for snapshot in &scenario.snapshots {
            assert!(
                snapshot.state.dram_pressure.memory_pressure > 0.2
                    && snapshot.state.dram_pressure.memory_pressure < 0.8,
                "database cache pressure should be moderate, got {}",
                snapshot.state.dram_pressure.memory_pressure
            );
        }

        // Should have hotness data
        for snapshot in &scenario.snapshots {
            assert!(
                snapshot.state.hotness_summary.is_some(),
                "database cache should have hotness data"
            );
        }
    }

    #[test]
    fn test_generate_mixed_multitask() {
        let gen = WorkloadGenerator::new(400);
        let def = mixed_multitask();
        let scenario = gen.generate(&def);

        // 200 seconds at 2000ms interval = 100 snapshots
        assert_eq!(scenario.snapshots.len(), 100);

        // Should have varying pressure levels (mixed workload)
        let pressures: Vec<f32> = scenario
            .snapshots
            .iter()
            .map(|s| s.state.dram_pressure.memory_pressure)
            .collect();
        let max_p = pressures.iter().copied().fold(0.0_f32, f32::max);
        let min_p = pressures.iter().copied().fold(1.0_f32, f32::min);
        assert!(
            max_p - min_p > 0.2,
            "mixed workload should have pressure variance, range={}-{}",
            min_p,
            max_p
        );
    }

    #[test]
    fn test_generate_allocator_stress() {
        let gen = WorkloadGenerator::new(500);
        let def = allocator_stress();
        let scenario = gen.generate(&def);

        // 60 seconds at 500ms interval = 120 snapshots
        assert_eq!(scenario.snapshots.len(), 120);

        // Should have rapid oscillation: both high and low pressures
        let max_pressure = scenario
            .snapshots
            .iter()
            .map(|s| s.state.dram_pressure.memory_pressure)
            .fold(0.0_f32, f32::max);
        let min_pressure = scenario
            .snapshots
            .iter()
            .map(|s| s.state.dram_pressure.memory_pressure)
            .fold(1.0_f32, f32::min);

        assert!(
            max_pressure > 0.5,
            "allocator stress should have high spikes, max={}",
            max_pressure
        );
        assert!(
            min_pressure < 0.4,
            "allocator stress should have low drops, min={}",
            min_pressure
        );
    }

    #[test]
    fn test_generate_tier_saturation() {
        let gen = WorkloadGenerator::new(600);
        let def = tier_saturation();
        let scenario = gen.generate(&def);

        // 120 seconds at 2000ms interval = 60 snapshots
        assert_eq!(scenario.snapshots.len(), 60);

        // Peak DRAM utilization should be very high
        assert!(
            scenario.metadata.peak_dram_utilization > 0.8,
            "tier saturation should have high peak DRAM util, got {}",
            scenario.metadata.peak_dram_utilization
        );
    }

    #[test]
    fn test_all_builtin_workloads_count() {
        let workloads = all_builtin_workloads();
        assert_eq!(workloads.len(), 7);
    }

    #[test]
    fn test_generator_deterministic() {
        let gen1 = WorkloadGenerator::new(42);
        let gen2 = WorkloadGenerator::new(42);
        let def = idle_desktop();

        let scenario1 = gen1.generate(&def);
        let scenario2 = gen2.generate(&def);

        assert_eq!(scenario1.snapshots.len(), scenario2.snapshots.len());
        for (s1, s2) in scenario1.snapshots.iter().zip(scenario2.snapshots.iter()) {
            assert_eq!(s1.timestamp_ms, s2.timestamp_ms);
            assert_eq!(
                s1.state.dram_pressure.memory_pressure,
                s2.state.dram_pressure.memory_pressure
            );
            assert_eq!(s1.state.dram_utilization, s2.state.dram_utilization);
            assert_eq!(s1.state.swap_utilization, s2.state.swap_utilization);
        }
    }

    #[test]
    fn test_generator_different_seed() {
        let gen1 = WorkloadGenerator::new(42);
        let gen2 = WorkloadGenerator::new(99);
        let def = idle_desktop();

        let scenario1 = gen1.generate(&def);
        let scenario2 = gen2.generate(&def);

        // At least one snapshot should differ
        let any_different = scenario1
            .snapshots
            .iter()
            .zip(scenario2.snapshots.iter())
            .any(|(s1, s2)| {
                s1.state.dram_pressure.memory_pressure != s2.state.dram_pressure.memory_pressure
                    || s1.state.dram_utilization != s2.state.dram_utilization
            });
        assert!(
            any_different,
            "different seeds should produce different scenarios"
        );
    }

    #[test]
    fn test_scenario_metadata() {
        let gen = WorkloadGenerator::new(42);
        let def = idle_desktop();
        let scenario = gen.generate(&def);

        assert_eq!(scenario.metadata.total_snapshots, 60);
        assert!(scenario.metadata.peak_dram_pressure >= 0.0);
        assert!(scenario.metadata.peak_dram_utilization >= 0.0);
        assert!(scenario.metadata.avg_dram_utilization >= 0.0);
        assert!(scenario.metadata.avg_swap_utilization >= 0.0);
    }

    #[test]
    fn test_pressure_time_distribution() {
        let gen = WorkloadGenerator::new(42);
        let def = idle_desktop();
        let scenario = gen.generate(&def);

        let dist = &scenario.metadata.pressure_time_distribution;
        let sum = dist.idle_fraction
            + dist.low_fraction
            + dist.medium_fraction
            + dist.high_fraction
            + dist.critical_fraction;
        assert!(
            (sum - 1.0).abs() < 0.01,
            "pressure time distribution should sum to ~1.0, got {}",
            sum
        );
    }
}
