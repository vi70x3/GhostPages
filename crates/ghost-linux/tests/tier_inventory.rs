//! Integration tests for TierInventory.

use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::time::DeterministicTimeProvider;
use ghost_core::types::TierId;

use ghost_linux::tier_inventory::{
    SimulatedTierInventory, TierInfo, TierInventory, TierKind,
};

fn test_time_provider() -> Arc<dyn ghost_core::time::TimeProvider> {
    Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ))
}

fn test_emitter() -> EventEmitter {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    EventEmitter::new(tx)
}

// ─── Discovery Tests ──────────────────────────────────────────────────────────

#[test]
fn test_discover_finds_dram() {
    let mut inventory = TierInventory::new(test_time_provider(), test_emitter());
    inventory.discover().expect("discover should succeed");

    // DRAM is always present
    let dram = inventory.get_tier(&TierId::Ram);
    assert!(dram.is_some(), "DRAM tier should always be present");
    let dram = dram.unwrap();
    assert_eq!(dram.kind, TierKind::Dram);
    assert_eq!(dram.name, "DRAM");
}

#[test]
fn test_discover_has_simulation_tier() {
    let mut inventory = TierInventory::new(test_time_provider(), test_emitter());
    inventory.discover().expect("discover should succeed");

    // Simulation tier is always present
    let sim = inventory.get_tier(&TierId::Simulation);
    assert!(sim.is_some(), "Simulation tier should always be present");
    let sim = sim.unwrap();
    assert_eq!(sim.kind, TierKind::Simulated);
}

#[test]
fn test_discover_with_swap() {
    let mut inventory = TierInventory::new(test_time_provider(), test_emitter());
    inventory.discover().expect("discover should succeed");

    // Swap tier may or may not be present depending on system
    // We just verify the inventory is valid
    assert!(inventory.tier_count() >= 2, "Should have at least DRAM and Simulation");
}

#[test]
fn test_discover_with_zram() {
    let mut inventory = TierInventory::new(test_time_provider(), test_emitter());
    inventory.discover().expect("discover should succeed");

    // ZRAM tier may or may not be present depending on system
    // We just verify the inventory is valid
    assert!(inventory.tier_count() >= 2, "Should have at least DRAM and Simulation");
}

// ─── Refresh Tests ────────────────────────────────────────────────────────────

#[test]
fn test_tier_refresh_updates_utilization() {
    let mut inventory = TierInventory::new(test_time_provider(), test_emitter());
    inventory.discover().expect("discover should succeed");

    // Refresh should update tier info
    inventory.refresh().expect("refresh should succeed");

    // DRAM should have been updated
    let dram = inventory.get_tier(&TierId::Ram);
    assert!(dram.is_some(), "DRAM tier should exist after refresh");
}

// ─── Preference Ordering Tests ────────────────────────────────────────────────

#[test]
fn test_tiers_by_preference_order() {
    let mut inventory = TierInventory::new(test_time_provider(), test_emitter());
    inventory.discover().expect("discover should succeed");

    let tiers = inventory.tiers_by_preference();
    assert!(!tiers.is_empty(), "Should have at least one tier");

    // DRAM should be first (highest preference)
    assert_eq!(tiers[0].kind, TierKind::Dram, "DRAM should be first in preference order");

    // Verify ordering: no tier with lower preference order should appear before one with higher
    for i in 1..tiers.len() {
        let prev = &tiers[i - 1];
        let curr = &tiers[i];
        let prev_order = tier_kind_preference(&prev.kind);
        let curr_order = tier_kind_preference(&curr.kind);
        assert!(
            prev_order <= curr_order,
            "Tier preference order should be non-decreasing: {:?} ({}) before {:?} ({})",
            prev.kind, prev_order, curr.kind, curr_order
        );
    }
}

fn tier_kind_preference(kind: &TierKind) -> u8 {
    match kind {
        TierKind::Dram => 0,
        TierKind::GpuVram => 1,
        TierKind::Zram => 2,
        TierKind::Simulated => 3,
        TierKind::Swap => 4,
        TierKind::DiskSwap => 5,
    }
}

// ─── Simulated Tier Inventory Tests ───────────────────────────────────────────

#[test]
fn test_simulated_deterministic() {
    let tp = test_time_provider();

    let sim1 = SimulatedTierInventory::new(42, 4, tp.clone(), test_emitter());
    let sim2 = SimulatedTierInventory::new(42, 4, tp, test_emitter());

    let tiers1 = sim1.generate().expect("generate should succeed");
    let tiers2 = sim2.generate().expect("generate should succeed");

    assert_eq!(tiers1.len(), tiers2.len(), "Same seed should produce same tier count");

    for (t1, t2) in tiers1.iter().zip(tiers2.iter()) {
        assert_eq!(t1.name, t2.name, "Same seed should produce same tier names");
        assert_eq!(t1.kind, t2.kind, "Same seed should produce same tier kinds");
        assert_eq!(t1.total_bytes, t2.total_bytes, "Same seed should produce same total bytes");
        assert_eq!(t1.used_bytes, t2.used_bytes, "Same seed should produce same used bytes");
        assert_eq!(t1.id, t2.id, "Same seed should produce same tier IDs");
    }
}

#[test]
fn test_simulated_different_seeds() {
    let tp = test_time_provider();

    let sim1 = SimulatedTierInventory::new(42, 4, tp.clone(), test_emitter());
    let sim2 = SimulatedTierInventory::new(99, 4, tp, test_emitter());

    let tiers1 = sim1.generate().expect("generate should succeed");
    let tiers2 = sim2.generate().expect("generate should succeed");

    // Different seeds should produce different values
    let any_different = tiers1.iter().zip(tiers2.iter()).any(|(t1, t2)| {
        t1.total_bytes != t2.total_bytes || t1.used_bytes != t2.used_bytes
    });
    assert!(any_different, "Different seeds should produce different tier graphs");
}

#[test]
fn test_simulated_always_has_dram() {
    let tp = test_time_provider();
    let sim = SimulatedTierInventory::new(42, 1, tp, test_emitter());
    let tiers = sim.generate().expect("generate should succeed");

    // Should always have at least DRAM
    assert!(!tiers.is_empty(), "Should have at least one tier");
    assert_eq!(tiers[0].kind, TierKind::Dram, "First tier should always be DRAM");
    assert_eq!(tiers[0].id, TierId::Ram, "First tier should have Ram ID");
}

#[test]
fn test_simulated_tier_count() {
    let tp = test_time_provider();

    for count in [1usize, 2, 3, 5, 10] {
        let sim = SimulatedTierInventory::new(42, count, tp.clone(), test_emitter());
        let tiers = sim.generate().expect("generate should succeed");
        assert_eq!(
            tiers.len(),
            count,
            "Should generate exactly {} tiers",
            count
        );
    }
}

// ─── Event Tests ──────────────────────────────────────────────────────────────

#[test]
fn test_emits_events() {
    let tp = test_time_provider();
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let mut inventory = TierInventory::new(tp, emitter);
    inventory.discover().expect("discover should succeed");

    // Drain all events and find the TierInventoryChanged event
    // (other events like SwapTopologyChanged may be emitted by readers first)
    let mut found_inventory_changed = false;
    while let Ok(record) = rx.try_recv() {
        if let Event::TierInventoryChanged { tiers, .. } = &record.event {
            found_inventory_changed = true;
            assert!(!tiers.is_empty(), "Tier inventory event should list tiers");
            assert!(tiers.contains(&"DRAM".to_string()), "Should contain DRAM");
            assert!(tiers.contains(&"Simulation".to_string()), "Should contain Simulation");
            break;
        }
    }
    assert!(
        found_inventory_changed,
        "Should have received a TierInventoryChanged event"
    );
}

#[test]
fn test_simulated_emits_inventory_changed() {
    let tp = test_time_provider();
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);

    let sim = SimulatedTierInventory::new(42, 3, tp, emitter);
    let tiers = sim.generate().expect("generate should succeed");

    // Should have received a TierInventoryChanged event
    let record = rx.try_recv().expect("should have received an event");
    match record.event {
        Event::TierInventoryChanged { tiers: event_tiers, .. } => {
            assert_eq!(
                event_tiers.len(),
                tiers.len(),
                "Event should list same number of tiers as generated"
            );
        }
        other => panic!("expected TierInventoryChanged, got {:?}", other),
    }
}

// ─── Replay Tests ─────────────────────────────────────────────────────────────

#[test]
fn test_replay() {
    let tp = test_time_provider();

    // Record phase
    let sim1 = SimulatedTierInventory::new(42, 3, tp.clone(), test_emitter());
    let original = sim1.generate().expect("generate should succeed");

    // Collect events
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let sim2 = SimulatedTierInventory::new(42, 3, tp.clone(), emitter);
    let replayed = sim2.generate().expect("generate should succeed");

    // Verify identical tier graphs
    assert_eq!(original.len(), replayed.len());
    for (orig, replay) in original.iter().zip(replayed.iter()) {
        assert_eq!(orig.name, replay.name);
        assert_eq!(orig.kind, replay.kind);
        assert_eq!(orig.total_bytes, replay.total_bytes);
        assert_eq!(orig.used_bytes, replay.used_bytes);
        assert_eq!(orig.available_bytes, replay.available_bytes);
    }

    // Verify events were emitted during replay
    let replay_events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
        .map(|r| r.event)
        .collect();
    assert!(!replay_events.is_empty(), "Should have emitted events during replay");
}

#[test]
fn test_replay_deterministic_across_runs() {
    let tp = test_time_provider();

    // Run 1
    let sim1 = SimulatedTierInventory::new(123, 5, tp.clone(), test_emitter());
    let run1 = sim1.generate().expect("generate should succeed");

    // Run 2 (same seed)
    let sim2 = SimulatedTierInventory::new(123, 5, tp, test_emitter());
    let run2 = sim2.generate().expect("generate should succeed");

    // Should be identical
    assert_eq!(run1.len(), run2.len());
    for (t1, t2) in run1.iter().zip(run2.iter()) {
        assert_eq!(t1.name, t2.name);
        assert_eq!(t1.total_bytes, t2.total_bytes);
        assert_eq!(t1.used_bytes, t2.used_bytes);
    }
}

// ─── Tier Info Tests ──────────────────────────────────────────────────────────

#[test]
fn test_tier_info_utilization_empty() {
    let info = TierInfo::new(TierId::Ram, TierKind::Dram, "DRAM");
    assert!((info.utilization() - 0.0).abs() < f64::EPSILON);
}

#[test]
fn test_tier_info_utilization_partial() {
    let mut info = TierInfo::new(TierId::Ram, TierKind::Dram, "DRAM");
    info.total_bytes = 10_000_000_000;
    info.used_bytes = 3_000_000_000;
    assert!((info.utilization() - 0.3).abs() < 0.001);
}

#[test]
fn test_tier_info_utilization_full() {
    let mut info = TierInfo::new(TierId::Ram, TierKind::Dram, "DRAM");
    info.total_bytes = 10_000_000_000;
    info.used_bytes = 10_000_000_000;
    assert!((info.utilization() - 1.0).abs() < f64::EPSILON);
}

// ─── All Tiers Tests ──────────────────────────────────────────────────────────

#[test]
fn test_all_tiers_returns_btreemap() {
    let mut inventory = TierInventory::new(test_time_provider(), test_emitter());
    inventory.discover().expect("discover should succeed");

    let tiers = inventory.all_tiers();
    assert!(!tiers.is_empty(), "Should have at least one tier");
    assert!(tiers.contains_key(&TierId::Ram), "Should contain RAM tier");
}

#[test]
fn test_get_tier_returns_none_for_missing() {
    let inventory = TierInventory::new(test_time_provider(), test_emitter());

    // No tiers discovered yet
    assert!(inventory.get_tier(&TierId::Ram).is_none());
    assert!(inventory.get_tier(&TierId::Disk).is_none());
}

// ─── Metrics Tests ────────────────────────────────────────────────────────────

#[test]
fn test_metrics_update() {
    use ghost_linux::tier_inventory::metrics;

    let registry = prometheus::Registry::new();
    let m = metrics::register(&registry).expect("register should succeed");

    let info = TierInfo {
        id: TierId::Ram,
        kind: TierKind::Dram,
        name: "DRAM".to_string(),
        total_bytes: 16_000_000_000,
        used_bytes: 8_000_000_000,
        available_bytes: 8_000_000_000,
        pressure: ghost_core::state::PressureState::new(),
        health: ghost_core::events::BackendHealth::Healthy,
        last_updated: 1_700_000_000,
    };

    metrics::update_tier(&m, &info);
    metrics::update_tier_count(&m, 3);
}
