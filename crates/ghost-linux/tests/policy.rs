//! Integration tests for the policy runtime.
//!
//! These tests verify that [`PolicyRuntime`] correctly evaluates system state
//! and emits deterministic, read-only recommendations.

use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::state::PressureState;
use ghost_core::time::{DeterministicTimeProvider, TimeProvider};
use ghost_core::types::{ChunkId, TierId};

use ghost_linux::policy::{PolicyRuntime, Recommendation};
use ghost_linux::policy_rules::{PolicyRules, SystemState};
use ghost_linux::tier_inventory::TierInventory;

// ─── Helpers ────────────────────────────────────────────────────────────────────

fn test_time_provider(start_secs: u64) -> Arc<dyn TimeProvider> {
    Arc::new(DeterministicTimeProvider::new(
        start_secs,
        std::time::Duration::from_secs(1),
    ))
}

fn test_emitter() -> (EventEmitter, tokio::sync::mpsc::Receiver<ghost_core::events::EventRecord>) {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    (EventEmitter::new(tx), rx)
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

/// Test: idle system produces NoAction recommendation.
#[test]
fn test_evaluate_idle_system() {
    let rules = PolicyRules::new();
    let state = SystemState {
        dram_pressure: PressureState::new(),
        dram_utilization: 0.3,
        swap_utilization: 0.1,
        zram_utilization: Some(0.2),
        io_pressure: PressureState::new(),
    };

    let recs = rules.evaluate(&state);

    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::NoAction { .. })),
        "expected NoAction for idle system, got {:?}",
        recs
    );
}

/// Test: high DRAM pressure produces MoveToZram or MoveToDiskSwap.
#[test]
fn test_evaluate_high_dram_pressure() {
    let rules = PolicyRules::new();
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
    };

    let recs = rules.evaluate(&state);

    // With ZRAM available, should recommend MoveToZram
    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
        "expected MoveToZram for high DRAM pressure with ZRAM, got {:?}",
        recs
    );
}

/// Test: critical DRAM pressure produces EvictCold + MoveToZram.
#[test]
fn test_evaluate_critical_dram_pressure() {
    let rules = PolicyRules::new();
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.95,
            ..Default::default()
        },
        dram_utilization: 0.97,
        swap_utilization: 0.5,
        zram_utilization: Some(0.6),
        io_pressure: PressureState::new(),
    };

    let recs = rules.evaluate(&state);

    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::EvictCold { .. })),
        "expected EvictCold for critical DRAM pressure, got {:?}",
        recs
    );
    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
        "expected MoveToZram for critical DRAM pressure with ZRAM, got {:?}",
        recs
    );
}

/// Test: ZRAM is preferred over disk swap when available.
#[test]
fn test_evaluate_with_zram_available() {
    let rules = PolicyRules::new();
    let state_with_zram = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
    };

    let state_without_zram = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: None,
        io_pressure: PressureState::new(),
    };

    let recs_with = rules.evaluate(&state_with_zram);
    let recs_without = rules.evaluate(&state_without_zram);

    // With ZRAM: should have MoveToZram
    assert!(
        recs_with
            .iter()
            .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
        "expected MoveToZram when ZRAM available, got {:?}",
        recs_with
    );

    // Without ZRAM: should have MoveToDiskSwap instead
    assert!(
        recs_without
            .iter()
            .any(|r| matches!(r, Recommendation::MoveToDiskSwap { .. })),
        "expected MoveToDiskSwap when no ZRAM, got {:?}",
        recs_without
    );
}

/// Test: same state produces same recommendations (determinism).
#[test]
fn test_evaluate_deterministic() {
    let rules = PolicyRules::new();
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
    };

    let recs1 = rules.evaluate(&state);
    let recs2 = rules.evaluate(&state);

    assert_eq!(
        recs1.len(),
        recs2.len(),
        "deterministic evaluation must produce same number of recommendations"
    );

    for (r1, r2) in recs1.iter().zip(recs2.iter()) {
        assert_eq!(
            std::mem::discriminant(r1),
            std::mem::discriminant(r2),
            "deterministic evaluation must produce same recommendation types"
        );
    }
}

/// Test: PolicyRuntime emits PolicyRecommendationGenerated events.
#[test]
fn test_evaluate_emits_events() {
    let time_provider = test_time_provider(1_700_000_000);
    let (emitter, mut rx) = test_emitter();

    let inventory = TierInventory::new(time_provider.clone(), emitter.clone());
    let inventory = Arc::new(parking_lot::RwLock::new(inventory));

    let runtime = PolicyRuntime::new(inventory, emitter, time_provider);
    let _recs = runtime.evaluate().expect("evaluate should succeed");

    // Drain events until we find PolicyRecommendationGenerated.
    // The PSI reader may emit MemoryPressureChanged events first.
    let mut found = false;
    for _ in 0..10 {
        match rx.try_recv() {
            Ok(record) => {
                if let Event::PolicyRecommendationGenerated {
                    recommendations,
                    pressure_level,
                    ..
                } = record.event
                {
                    assert!(!recommendations.is_empty(), "should have recommendations");
                    assert!(!pressure_level.is_empty(), "should have pressure level");
                    found = true;
                    break;
                }
                // Otherwise it's a PSI event — keep draining
            }
            Err(_) => break,
        }
    }
    assert!(
        found,
        "should have received a PolicyRecommendationGenerated event"
    );
}

/// Test: record/replay produces same recommendations.
#[test]
fn test_evaluate_replay() {
    let rules = PolicyRules::new();
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
    };

    // "Record" — evaluate once
    let recs_recorded = rules.evaluate(&state);

    // "Replay" — evaluate again with same state
    let recs_replayed = rules.evaluate(&state);

    // Must be identical
    assert_eq!(recs_recorded.len(), recs_replayed.len());
    for (r1, r2) in recs_recorded.iter().zip(recs_replayed.iter()) {
        assert_eq!(r1.kind(), r2.kind());
        assert_eq!(r1.reason(), r2.reason());
    }
}

/// Test: cooldown prevents rapid successive evaluations.
#[test]
fn test_cooldown_prevents_rapid_recommendations() {
    let time_provider = test_time_provider(1_700_000_000);
    let (emitter, _rx) = test_emitter();

    let inventory = TierInventory::new(time_provider.clone(), emitter.clone());
    let inventory = Arc::new(parking_lot::RwLock::new(inventory));

    let runtime = PolicyRuntime::new(inventory, emitter, time_provider);

    // Initially, cooldown should be expired (last_evaluation = 0)
    assert!(runtime.is_cooldown_expired());

    // After evaluation, cooldown should be active
    let _ = runtime.evaluate();
    assert!(
        !runtime.is_cooldown_expired(),
        "cooldown should be active after evaluation"
    );
}

/// Test: evaluate() doesn't mutate any state.
#[test]
fn test_no_mutation() {
    let time_provider = test_time_provider(1_700_000_000);
    let (emitter, _rx) = test_emitter();

    let inventory = TierInventory::new(time_provider.clone(), emitter.clone());
    let inventory = Arc::new(parking_lot::RwLock::new(inventory));

    // Capture initial state
    let initial_tier_count = inventory.read().tier_count();

    let runtime = PolicyRuntime::new(inventory.clone(), emitter, time_provider);

    // Evaluate multiple times
    let _ = runtime.evaluate();
    let _ = runtime.evaluate(); // Second call — cooldown active, but that's ok
    let _ = runtime.evaluate();

    // Verify inventory was not mutated
    let final_tier_count = inventory.read().tier_count();
    assert_eq!(
        initial_tier_count, final_tier_count,
        "evaluate() must not mutate tier inventory"
    );
}

/// Test: critical pressure without ZRAM falls back to disk swap.
#[test]
fn test_critical_pressure_no_zram_uses_disk_swap() {
    let rules = PolicyRules::new();
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.95,
            ..Default::default()
        },
        dram_utilization: 0.97,
        swap_utilization: 0.3,
        zram_utilization: None,
        io_pressure: PressureState::new(),
    };

    let recs = rules.evaluate(&state);

    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::EvictCold { .. })),
        "expected EvictCold for critical pressure, got {:?}",
        recs
    );
    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::MoveToDiskSwap { .. })),
        "expected MoveToDiskSwap for critical pressure without ZRAM, got {:?}",
        recs
    );
}

/// Test: high pressure with ZRAM full falls back to disk swap.
#[test]
fn test_high_pressure_zram_full_uses_disk_swap() {
    let rules = PolicyRules::new();
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.8,
            ..Default::default()
        },
        dram_utilization: 0.85,
        swap_utilization: 0.3,
        zram_utilization: Some(0.95), // ZRAM is nearly full
        io_pressure: PressureState::new(),
    };

    let recs = rules.evaluate(&state);

    // Should NOT recommend MoveToZram since ZRAM is full
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, Recommendation::MoveToZram { .. })),
        "should not recommend MoveToZram when ZRAM is full, got {:?}",
        recs
    );
}

/// Test: medium pressure produces demotion recommendations.
#[test]
fn test_medium_pressure_demotes() {
    let rules = PolicyRules::new();
    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.6,
            ..Default::default()
        },
        dram_utilization: 0.65,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
    };

    let recs = rules.evaluate(&state);

    assert!(
        recs.iter()
            .any(|r| matches!(r, Recommendation::DemoteHot { .. })),
        "expected DemoteHot for medium pressure, got {:?}",
        recs
    );
}

/// Test: custom thresholds change behavior.
#[test]
fn test_custom_thresholds() {
    // Low threshold — triggers high pressure at lower utilization
    let strict_rules = PolicyRules::with_thresholds(0.5, 0.7, 0.6, 0.6, 60);
    let lenient_rules = PolicyRules::with_thresholds(0.9, 0.95, 0.95, 0.95, 60);

    let state = SystemState {
        dram_pressure: PressureState {
            memory_pressure: 0.6,
            ..Default::default()
        },
        dram_utilization: 0.65,
        swap_utilization: 0.3,
        zram_utilization: Some(0.4),
        io_pressure: PressureState::new(),
    };

    let strict_recs = strict_rules.evaluate(&state);
    let lenient_recs = lenient_rules.evaluate(&state);

    // Strict rules should trigger more recommendations at this pressure level
    assert!(
        strict_recs.len() >= lenient_recs.len(),
        "strict rules should produce more or equal recommendations: strict={:?}, lenient={:?}",
        strict_recs,
        lenient_recs
    );
}

/// Test: Recommendation::kind() returns correct strings.
#[test]
fn test_recommendation_kind_strings() {
    let cases = vec![
        (
            Recommendation::PromoteToDram {
                chunk_id: ChunkId::from_data(b"test"),
                reason: "hot".into(),
            },
            "promote_to_dram",
        ),
        (
            Recommendation::MoveToZram {
                chunk_id: ChunkId::from_data(b"test"),
                reason: "cold".into(),
            },
            "move_to_zram",
        ),
        (
            Recommendation::MoveToDiskSwap {
                chunk_id: ChunkId::from_data(b"test"),
                reason: "cold".into(),
            },
            "move_to_disk_swap",
        ),
        (
            Recommendation::NoAction {
                reason: "idle".into(),
            },
            "no_action",
        ),
        (
            Recommendation::EvictCold {
                tier: TierId::Ram,
                count: 4,
            },
            "evict_cold",
        ),
        (
            Recommendation::DemoteHot {
                tier: TierId::GpuVram,
                target: TierId::Disk,
            },
            "demote_hot",
        ),
    ];

    for (rec, expected) in cases {
        assert_eq!(rec.kind(), expected, "wrong kind for {:?}", rec);
    }
}

/// Test: Recommendation::reason() returns the reason string.
#[test]
fn test_recommendation_reason_strings() {
    let rec = Recommendation::NoAction {
        reason: "system is idle".to_string(),
    };
    assert_eq!(rec.reason(), "system is idle");

    let rec = Recommendation::EvictCold {
        tier: TierId::Ram,
        count: 4,
    };
    assert_eq!(rec.reason(), "eviction due to pressure");
}

/// Test: serialization roundtrip for Recommendation.
#[test]
fn test_recommendation_serialization() {
    let rec = Recommendation::MoveToZram {
        chunk_id: ChunkId::from_data(b"test"),
        reason: "cold chunk".to_string(),
    };

    let json = serde_json::to_string(&rec).expect("serialize");
    let deserialized: Recommendation = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(rec.kind(), deserialized.kind());
    assert_eq!(rec.reason(), deserialized.reason());
}

/// Test: PolicyRuntime with custom rules.
#[test]
fn test_runtime_with_custom_rules() {
    let time_provider = test_time_provider(1_700_000_000);
    let (emitter, _rx) = test_emitter();

    let inventory = TierInventory::new(time_provider.clone(), emitter.clone());
    let inventory = Arc::new(parking_lot::RwLock::new(inventory));

    let rules = PolicyRules::with_thresholds(0.5, 0.7, 0.6, 0.6, 30);
    let runtime = PolicyRuntime::with_rules(inventory, emitter, time_provider, rules);

    let recs = runtime.evaluate().expect("evaluate should succeed");
    assert!(!recs.is_empty(), "should produce recommendations");
}

/// Test: multiple evaluations with same state are deterministic.
#[test]
fn test_runtime_deterministic_evaluations() {
    let time_provider = test_time_provider(1_700_000_000);
    let (emitter, _rx) = test_emitter();

    let inventory = TierInventory::new(time_provider.clone(), emitter.clone());
    let inventory = Arc::new(parking_lot::RwLock::new(inventory));

    let runtime = PolicyRuntime::new(inventory, emitter, time_provider);

    let recs1 = runtime.evaluate().expect("first evaluation");
    let recs2 = runtime.evaluate().expect("second evaluation");

    assert_eq!(recs1.len(), recs2.len());
    for (r1, r2) in recs1.iter().zip(recs2.iter()) {
        assert_eq!(r1.kind(), r2.kind());
    }
}
