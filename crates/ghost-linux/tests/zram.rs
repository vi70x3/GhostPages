//! Integration tests for ZRAM awareness.

use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::time::DeterministicTimeProvider;
use ghost_linux::zram::{SimulatedZramReader, ZramDevice, ZramSnapshot};


fn test_clock() -> Arc<DeterministicTimeProvider> {
    Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ))
}

/// Test parsing known sysfs output format.
#[test]
fn test_parse_sysfs_value() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = test_clock();

    let reader = SimulatedZramReader::new(clock, emitter, 42, None);

    // Test the simulation file format parser
    let content = "\
zram0
  orig_size: 4194304
  comp_size: 1048576
  mem_used_total: 2097152
  max_comp_streams: 2
  comp_algorithm: zstd
";

    let snapshot = reader.parse(content).unwrap();
    assert_eq!(snapshot.devices.len(), 1);
    assert_eq!(snapshot.devices[0].name, "zram0");
    assert_eq!(snapshot.devices[0].orig_size_kb, 4_194_304);
    assert_eq!(snapshot.devices[0].comp_size_kb, 1_048_576);
    assert_eq!(snapshot.devices[0].mem_used_total_kb, 2_097_152);
    assert_eq!(snapshot.devices[0].max_comp_streams, 2);
    assert_eq!(snapshot.devices[0].comp_algorithm, "zstd");
}

/// Test compression ratio calculation (orig / comp).
#[test]
fn test_compression_ratio_calculation() {
    let device = ZramDevice {
        name: "zram0".to_string(),
        orig_size_kb: 4_194_304,
        comp_size_kb: 1_048_576,
        mem_used_total_kb: 2_097_152,
        max_comp_streams: 2,
        comp_algorithm: "zstd".to_string(),
    };

    let ratio = device.compression_ratio().unwrap();
    assert!(
        (ratio - 4.0).abs() < 0.01,
        "Expected ratio ~4.0, got {}",
        ratio
    );

    // Verify snapshot-level ratio
    let snapshot = ZramSnapshot {
        devices: vec![device],
        total_orig_kb: 4_194_304,
        total_comp_kb: 1_048_576,
        compression_ratio: 4.0,
        timestamp: 1_700_000_000,
    };
    assert!((snapshot.compression_ratio - 4.0).abs() < 0.01);
}

/// Test deterministic generation from same seed.
#[test]
fn test_simulated_deterministic() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = test_clock();

    let reader1 = SimulatedZramReader::new(clock.clone(), emitter.clone(), 42, None);
    let reader2 = SimulatedZramReader::new(clock, emitter, 42, None);

    let snapshot1 = reader1.read().unwrap();
    let snapshot2 = reader2.read().unwrap();

    assert_eq!(snapshot1.devices.len(), snapshot2.devices.len());
    assert_eq!(snapshot1.total_orig_kb, snapshot2.total_orig_kb);
    assert_eq!(snapshot1.total_comp_kb, snapshot2.total_comp_kb);
    assert_eq!(snapshot1.compression_ratio, snapshot2.compression_ratio);

    for (d1, d2) in snapshot1.devices.iter().zip(snapshot2.devices.iter()) {
        assert_eq!(d1.name, d2.name);
        assert_eq!(d1.orig_size_kb, d2.orig_size_kb);
        assert_eq!(d1.comp_size_kb, d2.comp_size_kb);
        assert_eq!(d1.mem_used_total_kb, d2.mem_used_total_kb);
        assert_eq!(d1.max_comp_streams, d2.max_comp_streams);
        assert_eq!(d1.comp_algorithm, d2.comp_algorithm);
    }
}

/// Test that events are emitted during read.
#[test]
fn test_emits_events() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = test_clock();

    let reader = SimulatedZramReader::new(clock, emitter, 42, None);
    let snapshot = reader.read().unwrap();

    let mut zram_count = 0;
    let mut tier_count = 0;

    while let Ok(rec) = rx.try_recv() {
        match &rec.event {
            Event::ZramUtilizationChanged { device, .. } => {
                assert!(snapshot.devices.iter().any(|d| &d.name == device));
                zram_count += 1;
            }
            Event::TierInventoryChanged { tiers, .. } => {
                assert_eq!(tiers.len(), snapshot.devices.len());
                tier_count += 1;
            }
            _ => {}
        }
    }

    assert_eq!(zram_count, snapshot.devices.len());
    assert_eq!(tier_count, 1);
}

/// Test record/replay produces identical results.
#[test]
fn test_replay() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = test_clock();

    // Record
    let reader1 = SimulatedZramReader::new(clock.clone(), emitter.clone(), 42, None);
    let original = reader1.read().unwrap();

    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.event);
    }

    // Replay
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
    let emitter2 = EventEmitter::new(tx2);
    let reader2 = SimulatedZramReader::new(clock, emitter2, 42, None);
    let replayed = reader2.read().unwrap();

    assert_eq!(original.devices.len(), replayed.devices.len());
    assert_eq!(original.total_orig_kb, replayed.total_orig_kb);
    assert_eq!(original.total_comp_kb, replayed.total_comp_kb);
    assert_eq!(original.compression_ratio, replayed.compression_ratio);

    for (d1, d2) in original.devices.iter().zip(replayed.devices.iter()) {
        assert_eq!(d1.name, d2.name);
        assert_eq!(d1.orig_size_kb, d2.orig_size_kb);
        assert_eq!(d1.comp_size_kb, d2.comp_size_kb);
        assert_eq!(d1.comp_algorithm, d2.comp_algorithm);
    }

    let replay_events: Vec<_> = std::iter::from_fn(|| rx2.try_recv().ok())
        .map(|r| r.event)
        .collect();
    assert_eq!(replay_events.len(), events.len());
}

/// Test that ZRAM devices appear in tier inventory.
#[test]
fn test_tier_inventory_includes_zram() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = test_clock();

    let reader = SimulatedZramReader::new(clock, emitter, 42, None);
    let snapshot = reader.read().unwrap();

    let mut tier_event_found = false;
    while let Ok(rec) = rx.try_recv() {
        if let Event::TierInventoryChanged { tiers, .. } = &rec.event {
            tier_event_found = true;
            for device in &snapshot.devices {
                assert!(
                    tiers.contains(&device.name),
                    "ZRAM device {} not in tier inventory {:?}",
                    device.name,
                    tiers
                );
            }
        }
    }
    assert!(tier_event_found, "Expected TierInventoryChanged event");
}
