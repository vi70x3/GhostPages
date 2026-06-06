//! Integration tests for swap device discovery.
//!
//! Tests cover parsing, simulation, event emission, and replay for
//! `/proc/swaps` data source.

use std::io::Write;
use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::time::DeterministicTimeProvider;
use ghost_linux::swaps::{
    SimulatedSwapReader, SwapDevice, SwapKind, SwapReader, SwapTopology,
};
use tempfile::NamedTempFile;

// ─── Parse Tests ──────────────────────────────────────────────────────────────

#[test]
fn test_parse_swaps_file() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // Real /proc/swaps format
    let content = "\
Filename                                Type            Size    Used    Priority
/dev/sda2                               partition       8388608 0       -1
/swapfile                               file            2097152 1024    0";

    let topology = reader.parse(content).unwrap();

    assert_eq!(topology.devices.len(), 2);
    assert_eq!(topology.total_kb, 8_388_608 + 2_097_152);
    assert_eq!(topology.used_kb, 0 + 1024);

    // First device: partition
    assert_eq!(topology.devices[0].name, "/dev/sda2");
    assert_eq!(topology.devices[0].kind, SwapKind::Partition);
    assert_eq!(topology.devices[0].size_kb, 8_388_608);
    assert_eq!(topology.devices[0].used_kb, 0);
    assert_eq!(topology.devices[0].priority, -1);

    // Second device: file
    assert_eq!(topology.devices[1].name, "/swapfile");
    assert_eq!(topology.devices[1].kind, SwapKind::File);
    assert_eq!(topology.devices[1].size_kb, 2_097_152);
    assert_eq!(topology.devices[1].used_kb, 1024);
    assert_eq!(topology.devices[1].priority, 0);
}

#[test]
fn test_parse_swaps_file_with_trailing_whitespace() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    let content = "\
Filename                                Type            Size    Used    Priority
/dev/sda2                               partition       8388608 0       -1

";

    let topology = reader.parse(content).unwrap();
    assert_eq!(topology.devices.len(), 1);
    assert_eq!(topology.devices[0].name, "/dev/sda2");
}

#[test]
fn test_parse_swaps_empty() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // Only header, no devices
    let content = "\
Filename                                Type            Size    Used    Priority";

    let topology = reader.parse(content).unwrap();
    assert_eq!(topology.devices.len(), 0);
    assert_eq!(topology.total_kb, 0);
    assert_eq!(topology.used_kb, 0);
}

#[test]
fn test_parse_swaps_no_header() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // No header line — first line is data
    let content = "/dev/sda2                               partition       8388608 0       -1";

    let topology = reader.parse(content).unwrap();
    assert_eq!(topology.devices.len(), 1);
    assert_eq!(topology.devices[0].name, "/dev/sda2");
}

// ─── SwapKind Detection Tests ────────────────────────────────────────────────

#[test]
fn test_swap_kind_detection() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // Explicit type field
    let content = "\
Filename                                Type            Size    Used    Priority
/dev/sda2                               partition       8388608 0       -1
/swapfile                               file            2097152 1024    0";

    let topology = reader.parse(content).unwrap();
    assert_eq!(topology.devices[0].kind, SwapKind::Partition);
    assert_eq!(topology.devices[1].kind, SwapKind::File);

    // Fallback: /dev/ prefix → partition
    let content2 = "\
Filename                                Type            Size    Used    Priority
/dev/zram0                               device            4194304 512     10";

    let topology2 = reader.parse(content2).unwrap();
    assert_eq!(topology2.devices[0].kind, SwapKind::Partition);

    // Fallback: .swap in name → file
    let content3 = "\
Filename                                Type            Size    Used    Priority
/var/swap.swap                           device            1048576 0       5";

    let topology3 = reader.parse(content3).unwrap();
    assert_eq!(topology3.devices[0].kind, SwapKind::File);

    // Unknown type, unknown name
    let content4 = "\
Filename                                Type            Size    Used    Priority
/mystery                                device            1048576 0       5";

    let topology4 = reader.parse(content4).unwrap();
    assert_eq!(topology4.devices[0].kind, SwapKind::Unknown);
}

// ─── Deterministic Simulation Tests ──────────────────────────────────────────

#[test]
fn test_simulated_deterministic() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader1 = SimulatedSwapReader::new(
        clock.clone(),
        emitter.clone(),
        42,
        None,
    );
    let reader2 = SimulatedSwapReader::new(
        clock,
        emitter,
        42,
        None,
    );

    let topology1 = reader1.read().unwrap();
    let topology2 = reader2.read().unwrap();

    // Same seed must produce identical topology
    assert_eq!(topology1.devices.len(), topology2.devices.len());
    assert_eq!(topology1.total_kb, topology2.total_kb);
    assert_eq!(topology1.used_kb, topology2.used_kb);

    for (d1, d2) in topology1.devices.iter().zip(topology2.devices.iter()) {
        assert_eq!(d1.name, d2.name);
        assert_eq!(d1.kind, d2.kind);
        assert_eq!(d1.priority, d2.priority);
        assert_eq!(d1.size_kb, d2.size_kb);
        assert_eq!(d1.used_kb, d2.used_kb);
    }
}

#[test]
fn test_simulated_different_seeds() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader1 = SimulatedSwapReader::new(
        clock.clone(),
        emitter.clone(),
        42,
        None,
    );
    let reader2 = SimulatedSwapReader::new(
        clock,
        emitter,
        99,
        None,
    );

    let topology1 = reader1.read().unwrap();
    let topology2 = reader2.read().unwrap();

    // Different seeds should produce different topology
    assert_ne!(topology1.total_kb, topology2.total_kb);
}

#[test]
fn test_simulated_sorted_by_priority() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader = SimulatedSwapReader::new(clock, emitter, 42, None);
    let topology = reader.read().unwrap();

    // Devices should be sorted by priority descending
    for window in topology.devices.windows(2) {
        assert!(
            window[0].priority >= window[1].priority,
            "Devices should be sorted by priority descending: {:?} before {:?}",
            window[0],
            window[1]
        );
    }
}

// ─── Event Emission Tests ────────────────────────────────────────────────────

#[test]
fn test_emits_events() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader = SimulatedSwapReader::new(clock, emitter, 42, None);
    let topology = reader.read().unwrap();

    // Should have received SwapTopologyChanged + one SwapUtilizationChanged per device
    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.event);
    }

    // At least one SwapTopologyChanged
    let topology_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, Event::SwapTopologyChanged { .. }))
        .collect();
    assert_eq!(topology_events.len(), 1);

    // One SwapUtilizationChanged per device
    let utilization_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, Event::SwapUtilizationChanged { .. }))
        .collect();
    assert_eq!(utilization_events.len(), topology.devices.len());

    // Verify topology event content
    if let Event::SwapTopologyChanged {
        devices,
        total_kb,
        used_kb,
        ..
    } = &topology_events[0]
    {
        assert_eq!(devices.len(), topology.devices.len());
        assert_eq!(*total_kb, topology.total_kb);
        assert_eq!(*used_kb, topology.used_kb);
    }
}

#[test]
fn test_emits_events_real_reader() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter.clone(),
    );

    let content = "\
Filename                                Type            Size    Used    Priority
/dev/sda2                               partition       8388608 0       -1
/swapfile                               file            2097152 1024    0";

    // Parse the content
    let topology = reader.parse(content).unwrap();

    // Manually emit events (parse() doesn't emit, read() does)
    reader.emit_events(&topology);

    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.event);
    }

    // Should have SwapTopologyChanged + 2 SwapUtilizationChanged
    let topology_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, Event::SwapTopologyChanged { .. }))
        .collect();
    assert_eq!(topology_events.len(), 1);

    let utilization_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, Event::SwapUtilizationChanged { .. }))
        .collect();
    assert_eq!(utilization_events.len(), 2);

    // Verify utilization event for /dev/sda2
    let sda2_event = utilization_events
        .iter()
        .find(|e| matches!(e, Event::SwapUtilizationChanged { device, .. } if device == "/dev/sda2"))
        .expect("should have event for /dev/sda2");

    if let Event::SwapUtilizationChanged {
        device,
        used_kb,
        total_kb,
        ..
    } = sda2_event
    {
        assert_eq!(device, "/dev/sda2");
        assert_eq!(*used_kb, 0);
        assert_eq!(*total_kb, 8_388_608);
    }
}

// ─── Replay Tests ────────────────────────────────────────────────────────────

#[test]
fn test_replay() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    // Record phase
    let reader1 = SimulatedSwapReader::new(clock.clone(), emitter.clone(), 42, None);
    let original = reader1.read().unwrap();

    // Collect emitted events
    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.event);
    }

    // Replay phase — same seed should produce identical topology
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
    let emitter2 = EventEmitter::new(tx2);
    let reader2 = SimulatedSwapReader::new(clock, emitter2, 42, None);
    let replayed = reader2.read().unwrap();

    // Verify identical topology
    assert_eq!(original.devices.len(), replayed.devices.len());
    assert_eq!(original.total_kb, replayed.total_kb);
    assert_eq!(original.used_kb, replayed.used_kb);

    for (d1, d2) in original.devices.iter().zip(replayed.devices.iter()) {
        assert_eq!(d1.name, d2.name);
        assert_eq!(d1.kind, d2.kind);
        assert_eq!(d1.priority, d2.priority);
        assert_eq!(d1.size_kb, d2.size_kb);
        assert_eq!(d1.used_kb, d2.used_kb);
    }

    // Verify events were emitted during replay too
    let replay_events: Vec<_> = std::iter::from_fn(|| rx2.try_recv().ok())
        .map(|r| r.event)
        .collect();
    assert_eq!(replay_events.len(), events.len());
}

#[test]
fn test_replay_from_file() {
    let mut tmpfile = NamedTempFile::new().unwrap();
    writeln!(
        tmpfile,
        "Filename                                Type            Size    Used    Priority"
    )
    .unwrap();
    writeln!(
        tmpfile,
        "/dev/sda2                               partition       8388608 0       -1"
    )
    .unwrap();
    writeln!(
        tmpfile,
        "/swapfile                               file            2097152 1024    0"
    )
    .unwrap();

    // Read twice from same file
    let topology1 = SwapReader::read_from_file(tmpfile.path().to_str().unwrap()).unwrap();
    let topology2 = SwapReader::read_from_file(tmpfile.path().to_str().unwrap()).unwrap();

    assert_eq!(topology1.devices.len(), topology2.devices.len());
    assert_eq!(topology1.total_kb, topology2.total_kb);
    assert_eq!(topology1.used_kb, topology2.used_kb);

    for (d1, d2) in topology1.devices.iter().zip(topology2.devices.iter()) {
        assert_eq!(d1.name, d2.name);
        assert_eq!(d1.kind, d2.kind);
        assert_eq!(d1.size_kb, d2.size_kb);
        assert_eq!(d1.used_kb, d2.used_kb);
    }
}

// ─── Event Category Tests ────────────────────────────────────────────────────

#[test]
fn test_swap_topology_event_category() {
    let event = Event::SwapTopologyChanged {
        sequence_id: 0,
        devices: vec!["/dev/sda2".to_string()],
        total_kb: 8_388_608,
        used_kb: 0,
    };

    assert_eq!(event.category(), "swap");
    assert_eq!(event.event_name(), "swap_topology_changed");
}

#[test]
fn test_swap_utilization_event_category() {
    let event = Event::SwapUtilizationChanged {
        sequence_id: 0,
        device: "/dev/sda2".to_string(),
        used_kb: 1024,
        total_kb: 8_388_608,
    };

    assert_eq!(event.category(), "swap");
    assert_eq!(event.event_name(), "swap_utilization_changed");
}

// ─── SwapDevice Tests ────────────────────────────────────────────────────────

#[test]
fn test_swap_device_display() {
    let device = SwapDevice {
        name: "/dev/sda2".to_string(),
        kind: SwapKind::Partition,
        priority: -1,
        size_kb: 8_388_608,
        used_kb: 0,
    };

    assert_eq!(device.name, "/dev/sda2");
    assert_eq!(device.kind, SwapKind::Partition);
    assert_eq!(device.priority, -1);
}

#[test]
fn test_swap_topology_totals() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    let content = "\
Filename                                Type            Size    Used    Priority
/dev/sda2                               partition       8388608 100     -1
/swapfile                               file            2097152 500     0
/zram0                                  partition       4194304 200     10";

    let topology = reader.parse(content).unwrap();

    assert_eq!(topology.devices.len(), 3);
    assert_eq!(topology.total_kb, 8_388_608 + 2_097_152 + 4_194_304);
    assert_eq!(topology.used_kb, 100 + 500 + 200);
}

// ─── Edge Case Tests ─────────────────────────────────────────────────────────

#[test]
fn test_parse_swaps_with_missing_priority() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // Line with only 4 fields (no priority)
    let content = "\
Filename                                Type            Size    Used    Priority
/dev/sda2                               partition       8388608 0";

    let topology = reader.parse(content).unwrap();
    assert_eq!(topology.devices.len(), 1);
    assert_eq!(topology.devices[0].priority, 0); // default
}

#[test]
fn test_parse_swaps_with_non_numeric_values() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = SwapReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // Line with non-numeric size/used
    let content = "\
Filename                                Type            Size    Used    Priority
/dev/sda2                               partition       abc     xyz     -1";

    let topology = reader.parse(content).unwrap();
    assert_eq!(topology.devices.len(), 1);
    assert_eq!(topology.devices[0].size_kb, 0); // default on parse failure
    assert_eq!(topology.devices[0].used_kb, 0);
}

#[test]
fn test_simulated_from_file() {
    let mut tmpfile = NamedTempFile::new().unwrap();
    writeln!(
        tmpfile,
        "Filename                                Type            Size    Used    Priority"
    )
    .unwrap();
    writeln!(
        tmpfile,
        "/dev/sda2                               partition       8388608 0       -1"
    )
    .unwrap();

    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader = SimulatedSwapReader::new(
        clock,
        emitter,
        42,
        Some(tmpfile.path().to_str().unwrap().to_string()),
    );

    let topology = reader.read().unwrap();
    assert_eq!(topology.devices.len(), 1);
    assert_eq!(topology.devices[0].name, "/dev/sda2");
    assert_eq!(topology.devices[0].kind, SwapKind::Partition);
    assert_eq!(topology.devices[0].size_kb, 8_388_608);
}
