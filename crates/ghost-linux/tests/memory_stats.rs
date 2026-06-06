//! Integration tests for memory statistics collection.
//!
//! Tests cover parsing, simulation, event emission, and replay for
//! both `/proc/meminfo` and `/proc/vmstat` data sources.

use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::time::DeterministicTimeProvider;
use ghost_linux::meminfo::{MeminfoReader, MeminfoSnapshot, SimulatedMeminfoReader};
use ghost_linux::vmstat::{SimulatedVmstatReader, VmstatReader, VmstatSnapshot};

// ─── Meminfo Tests ───────────────────────────────────────────────────────────

#[test]
fn test_parse_meminfo() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = MeminfoReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    let content = "\
MemTotal:       16384000 kB
MemFree:         8192000 kB
MemAvailable:   12288000 kB
Buffers:          512000 kB
Cached:          4096000 kB
SwapTotal:       8388608 kB
SwapFree:        8388608 kB
Active:          6144000 kB
Inactive:        3072000 kB
Dirty:             16384 kB
Writeback:         8192 kB";

    let snapshot = reader.parse(content).unwrap();

    assert_eq!(snapshot.total_kb, 16_384_000);
    assert_eq!(snapshot.available_kb, 12_288_000);
    assert_eq!(snapshot.free_kb, 8_192_000);
    assert_eq!(snapshot.buffers_kb, 512_000);
    assert_eq!(snapshot.cached_kb, 4_096_000);
    assert_eq!(snapshot.swap_total_kb, 8_388_608);
    assert_eq!(snapshot.swap_free_kb, 8_388_608);
    assert_eq!(snapshot.active_kb, 6_144_000);
    assert_eq!(snapshot.inactive_kb, 3_072_000);
    assert_eq!(snapshot.dirty_kb, 16_384);
    assert_eq!(snapshot.writeback_kb, 8_192);
}

#[test]
fn test_parse_meminfo_with_extra_fields() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = MeminfoReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // Real /proc/meminfo has many more fields we should ignore
    let content = "\
MemTotal:       16384000 kB
MemFree:         8192000 kB
MemAvailable:   12288000 kB
Buffers:          512000 kB
Cached:          4096000 kB
SwapTotal:       8388608 kB
SwapFree:        8388608 kB
Active:          6144000 kB
Inactive:        3072000 kB
Dirty:             16384 kB
Writeback:         8192 kB
HugePages_Total:       0
HugePages_Free:        0
Hugepagesize:       2048 kB
Shmem:            256000 kB
Slab:             512000 kB
SReclaimable:     384000 kB
SUnreclaim:       128000 kB";

    let snapshot = reader.parse(content).unwrap();

    // Should still parse correctly, ignoring unknown fields
    assert_eq!(snapshot.total_kb, 16_384_000);
    assert_eq!(snapshot.available_kb, 12_288_000);
}

// ─── Vmstat Tests ────────────────────────────────────────────────────────────

#[test]
fn test_parse_vmstat() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = VmstatReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    let content = "\
pgscan_kswapd 12345
pgscan_direct 67890
pgsteal_kswapd 1234
pgsteal_direct 5678
oom_kill 0
pswpin 100
pswpout 200
pgfault 1234567
pgmajfault 890";

    let snapshot = reader.parse(content).unwrap();

    assert_eq!(snapshot.pgscan_kswapd, 12345);
    assert_eq!(snapshot.pgscan_direct, 67890);
    assert_eq!(snapshot.pgsteal_kswapd, 1234);
    assert_eq!(snapshot.pgsteal_direct, 5678);
    assert_eq!(snapshot.oom_kill, 0);
    assert_eq!(snapshot.pswpin, 100);
    assert_eq!(snapshot.pswpout, 200);
    assert_eq!(snapshot.pgfault, 1234567);
    assert_eq!(snapshot.pgmajfault, 890);
}

#[test]
fn test_parse_vmstat_with_extra_fields() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = VmstatReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // Real /proc/vmstat has many more fields we should ignore
    let content = "\
pgscan_kswapd 12345
pgscan_direct 67890
pgsteal_kswapd 1234
pgsteal_direct 5678
oom_kill 0
pswpin 100
pswpout 200
pgfault 1234567
pgmajfault 890
nr_free_pages 123456
nr_inactive_anon 789
nr_active_anon 456
nr_file_pages 101112
nr_dirty 131415
nr_writeback 161718";

    let snapshot = reader.parse(content).unwrap();

    // Should still parse correctly, ignoring unknown fields
    assert_eq!(snapshot.pgscan_kswapd, 12345);
    assert_eq!(snapshot.pgscan_direct, 67890);
    assert_eq!(snapshot.oom_kill, 0);
}

// ─── Deterministic Simulation Tests ─────────────────────────────────────────

#[test]
fn test_simulated_meminfo_deterministic() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader1 = SimulatedMeminfoReader::new(clock.clone(), emitter.clone(), 42, None);
    let reader2 = SimulatedMeminfoReader::new(clock, emitter, 42, None);

    let snapshot1 = reader1.read().unwrap();
    let snapshot2 = reader2.read().unwrap();

    // Same seed must produce identical values
    assert_eq!(snapshot1.total_kb, snapshot2.total_kb);
    assert_eq!(snapshot1.available_kb, snapshot2.available_kb);
    assert_eq!(snapshot1.free_kb, snapshot2.free_kb);
    assert_eq!(snapshot1.buffers_kb, snapshot2.buffers_kb);
    assert_eq!(snapshot1.cached_kb, snapshot2.cached_kb);
    assert_eq!(snapshot1.swap_total_kb, snapshot2.swap_total_kb);
    assert_eq!(snapshot1.swap_free_kb, snapshot2.swap_free_kb);
    assert_eq!(snapshot1.active_kb, snapshot2.active_kb);
    assert_eq!(snapshot1.inactive_kb, snapshot2.inactive_kb);
    assert_eq!(snapshot1.dirty_kb, snapshot2.dirty_kb);
    assert_eq!(snapshot1.writeback_kb, snapshot2.writeback_kb);
}

#[test]
fn test_simulated_vmstat_deterministic() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader1 = SimulatedVmstatReader::new(clock.clone(), emitter.clone(), 42, None);
    let reader2 = SimulatedVmstatReader::new(clock, emitter, 42, None);

    let snapshot1 = reader1.read().unwrap();
    let snapshot2 = reader2.read().unwrap();

    // Same seed must produce identical values
    assert_eq!(snapshot1.pgscan_kswapd, snapshot2.pgscan_kswapd);
    assert_eq!(snapshot1.pgscan_direct, snapshot2.pgscan_direct);
    assert_eq!(snapshot1.pgsteal_kswapd, snapshot2.pgsteal_kswapd);
    assert_eq!(snapshot1.pgsteal_direct, snapshot2.pgsteal_direct);
    assert_eq!(snapshot1.oom_kill, snapshot2.oom_kill);
    assert_eq!(snapshot1.pswpin, snapshot2.pswpin);
    assert_eq!(snapshot1.pswpout, snapshot2.pswpout);
    assert_eq!(snapshot1.pgfault, snapshot2.pgfault);
    assert_eq!(snapshot1.pgmajfault, snapshot2.pgmajfault);
}

// ─── Event Emission Tests ───────────────────────────────────────────────────

#[test]
fn test_meminfo_emits_events() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader = SimulatedMeminfoReader::new(clock, emitter, 42, None);
    let snapshot = reader.read().unwrap();

    // Should have received a MemoryStatsChanged event
    let record = rx.try_recv().expect("should have received an event");
    match record.event {
        Event::MemoryStatsChanged {
            sequence_id: 0,
            total_kb,
            available_kb,
            swap_used_kb,
            dirty_kb,
        } => {
            assert_eq!(total_kb, snapshot.total_kb);
            assert_eq!(available_kb, snapshot.available_kb);
            assert_eq!(dirty_kb, snapshot.dirty_kb);
            let expected_swap_used = snapshot.swap_total_kb - snapshot.swap_free_kb;
            assert_eq!(swap_used_kb, expected_swap_used);
        }
        other => panic!("expected MemoryStatsChanged, got {:?}", other),
    }
}

#[test]
fn test_vmstat_emits_events() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader = SimulatedVmstatReader::new(clock, emitter, 42, None);
    let snapshot = reader.read().unwrap();

    // Should have received a VmstatChanged event
    let record = rx.try_recv().expect("should have received an event");
    match record.event {
        Event::VmstatChanged {
            sequence_id: 0,
            pgscan_kswapd,
            pgscan_direct,
            oom_kill,
            pswpin,
            pswpout,
        } => {
            assert_eq!(pgscan_kswapd, snapshot.pgscan_kswapd);
            assert_eq!(pgscan_direct, snapshot.pgscan_direct);
            assert_eq!(oom_kill, snapshot.oom_kill);
            assert_eq!(pswpin, snapshot.pswpin);
            assert_eq!(pswpout, snapshot.pswpout);
        }
        other => panic!("expected VmstatChanged, got {:?}", other),
    }
}

// ─── Replay Tests ───────────────────────────────────────────────────────────

#[test]
fn test_meminfo_replay() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    // Record phase
    let reader1 = SimulatedMeminfoReader::new(clock.clone(), emitter.clone(), 42, None);
    let original = reader1.read().unwrap();

    // Collect emitted events
    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.event);
    }

    // Replay phase — same seed should produce identical values
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
    let emitter2 = EventEmitter::new(tx2);
    let reader2 = SimulatedMeminfoReader::new(clock, emitter2, 42, None);
    let replayed = reader2.read().unwrap();

    // Verify identical snapshots
    assert_eq!(original.total_kb, replayed.total_kb);
    assert_eq!(original.available_kb, replayed.available_kb);
    assert_eq!(original.free_kb, replayed.free_kb);
    assert_eq!(original.buffers_kb, replayed.buffers_kb);
    assert_eq!(original.cached_kb, replayed.cached_kb);
    assert_eq!(original.swap_total_kb, replayed.swap_total_kb);
    assert_eq!(original.swap_free_kb, replayed.swap_free_kb);
    assert_eq!(original.active_kb, replayed.active_kb);
    assert_eq!(original.inactive_kb, replayed.inactive_kb);
    assert_eq!(original.dirty_kb, replayed.dirty_kb);
    assert_eq!(original.writeback_kb, replayed.writeback_kb);

    // Verify events were emitted during replay too
    let replay_events: Vec<_> = std::iter::from_fn(|| rx2.try_recv().ok())
        .map(|r| r.event)
        .collect();
    assert_eq!(replay_events.len(), events.len());
}

#[test]
fn test_vmstat_replay() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    // Record phase
    let reader1 = SimulatedVmstatReader::new(clock.clone(), emitter.clone(), 42, None);
    let original = reader1.read().unwrap();

    // Collect emitted events
    let mut events = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        events.push(rec.event);
    }

    // Replay phase — same seed should produce identical values
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
    let emitter2 = EventEmitter::new(tx2);
    let reader2 = SimulatedVmstatReader::new(clock, emitter2, 42, None);
    let replayed = reader2.read().unwrap();

    // Verify identical snapshots
    assert_eq!(original.pgscan_kswapd, replayed.pgscan_kswapd);
    assert_eq!(original.pgscan_direct, replayed.pgscan_direct);
    assert_eq!(original.pgsteal_kswapd, replayed.pgsteal_kswapd);
    assert_eq!(original.pgsteal_direct, replayed.pgsteal_direct);
    assert_eq!(original.oom_kill, replayed.oom_kill);
    assert_eq!(original.pswpin, replayed.pswpin);
    assert_eq!(original.pswpout, replayed.pswpout);
    assert_eq!(original.pgfault, replayed.pgfault);
    assert_eq!(original.pgmajfault, replayed.pgmajfault);

    // Verify events were emitted during replay too
    let replay_events: Vec<_> = std::iter::from_fn(|| rx2.try_recv().ok())
        .map(|r| r.event)
        .collect();
    assert_eq!(replay_events.len(), events.len());
}

// ─── Event Category Tests ───────────────────────────────────────────────────

#[test]
fn test_memory_stats_event_category() {
    let event = Event::MemoryStatsChanged {
        sequence_id: 0,
        total_kb: 16_384_000,
        available_kb: 12_288_000,
        swap_used_kb: 0,
        dirty_kb: 16_384,
    };

    assert_eq!(event.category(), "memory");
    assert_eq!(event.event_name(), "memory_stats_changed");
}

#[test]
fn test_vmstat_event_category() {
    let event = Event::VmstatChanged {
        sequence_id: 0,
        pgscan_kswapd: 12345,
        pgscan_direct: 67890,
        oom_kill: 0,
        pswpin: 100,
        pswpout: 200,
    };

    assert_eq!(event.category(), "memory");
    assert_eq!(event.event_name(), "vmstat_changed");
}

// ─── Edge Case Tests ────────────────────────────────────────────────────────

#[test]
fn test_parse_empty_meminfo() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = MeminfoReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    let snapshot = reader.parse("").unwrap();

    // All fields should default to 0
    assert_eq!(snapshot.total_kb, 0);
    assert_eq!(snapshot.available_kb, 0);
    assert_eq!(snapshot.free_kb, 0);
    assert_eq!(snapshot.buffers_kb, 0);
    assert_eq!(snapshot.cached_kb, 0);
    assert_eq!(snapshot.swap_total_kb, 0);
    assert_eq!(snapshot.swap_free_kb, 0);
    assert_eq!(snapshot.active_kb, 0);
    assert_eq!(snapshot.inactive_kb, 0);
    assert_eq!(snapshot.dirty_kb, 0);
    assert_eq!(snapshot.writeback_kb, 0);
}

#[test]
fn test_parse_empty_vmstat() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = VmstatReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    let snapshot = reader.parse("").unwrap();

    // All fields should default to 0
    assert_eq!(snapshot.pgscan_kswapd, 0);
    assert_eq!(snapshot.pgscan_direct, 0);
    assert_eq!(snapshot.pgsteal_kswapd, 0);
    assert_eq!(snapshot.pgsteal_direct, 0);
    assert_eq!(snapshot.oom_kill, 0);
    assert_eq!(snapshot.pswpin, 0);
    assert_eq!(snapshot.pswpout, 0);
    assert_eq!(snapshot.pgfault, 0);
    assert_eq!(snapshot.pgmajfault, 0);
}

#[test]
fn test_simulated_different_seeds_meminfo() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader1 = SimulatedMeminfoReader::new(clock.clone(), emitter.clone(), 42, None);
    let reader2 = SimulatedMeminfoReader::new(clock, emitter, 99, None);

    let snapshot1 = reader1.read().unwrap();
    let snapshot2 = reader2.read().unwrap();

    // Different seeds should produce different values
    assert_ne!(snapshot1.total_kb, snapshot2.total_kb);
}

#[test]
fn test_simulated_different_seeds_vmstat() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(
        1_700_000_000,
        std::time::Duration::from_secs(1),
    ));

    let reader1 = SimulatedVmstatReader::new(clock.clone(), emitter.clone(), 42, None);
    let reader2 = SimulatedVmstatReader::new(clock, emitter, 99, None);

    let snapshot1 = reader1.read().unwrap();
    let snapshot2 = reader2.read().unwrap();

    // Different seeds should produce different values
    assert_ne!(snapshot1.pgscan_kswapd, snapshot2.pgscan_kswapd);
}
