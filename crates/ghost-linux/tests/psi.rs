//! Integration tests for PSI (Pressure Stall Information) module.

use std::sync::Arc;

use ghost_core::emitter::EventEmitter;
use ghost_core::events::Event;
use ghost_core::state::PressureState;
use ghost_core::time::DeterministicTimeProvider;

use ghost_linux::psi::{
    classify_pressure, pressure_level_str, PsiReader, PsiResource, SimulatedPsiReader,
};

// ─── Test: parse a known PSI line ──────────────────────────────────────────────

#[test]
fn test_parse_psi_line() {
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let reader = PsiReader::new(
        Arc::new(ghost_core::time::RealTimeProvider),
        emitter,
    );

    // Standard "some" line from /proc/pressure/memory
    let line = "some avg10=2.50 avg60=1.20 avg300=0.80 total=123456";
    let sample = reader.parse_line(line, PsiResource::Memory).unwrap();

    assert_eq!(sample.resource, PsiResource::Memory);
    assert!((sample.avg10 - 2.50).abs() < f64::EPSILON);
    assert!((sample.avg60 - 1.20).abs() < f64::EPSILON);
    assert!((sample.avg300 - 0.80).abs() < f64::EPSILON);
    assert_eq!(sample.total, 123456);
    assert!(sample.timestamp > 0);

    // "full" line (some PSI files report "full" instead of "some")
    let line = "full avg10=15.00 avg60=10.50 avg300=5.25 total=9999999";
    let sample = reader.parse_line(line, PsiResource::Io).unwrap();

    assert_eq!(sample.resource, PsiResource::Io);
    assert!((sample.avg10 - 15.00).abs() < f64::EPSILON);
    assert!((sample.avg60 - 10.50).abs() < f64::EPSILON);
    assert!((sample.avg300 - 5.25).abs() < f64::EPSILON);
    assert_eq!(sample.total, 9_999_999);

    // Zero values
    let line = "some avg10=0.00 avg60=0.00 avg300=0.00 total=0";
    let sample = reader.parse_line(line, PsiResource::Cpu).unwrap();
    assert!((sample.avg10).abs() < f64::EPSILON);
    assert_eq!(sample.total, 0);
}

// ─── Test: PSI → PressureState classification ─────────────────────────────────

#[test]
fn test_pressure_classification() {
    // Low: avg10 < 1.0
    let state = classify_pressure(0.0);
    assert_eq!(state.memory_pressure, 0.0);
    assert_eq!(pressure_level_str(0.0), "low");

    let state = classify_pressure(0.99);
    assert_eq!(state.memory_pressure, 0.0);
    assert_eq!(pressure_level_str(0.99), "low");

    // Medium: 1.0 <= avg10 < 5.0
    let state = classify_pressure(1.0);
    assert!((state.memory_pressure - 0.3).abs() < f32::EPSILON);
    assert_eq!(pressure_level_str(1.0), "medium");

    let state = classify_pressure(3.5);
    assert!((state.memory_pressure - 0.3).abs() < f32::EPSILON);
    assert_eq!(pressure_level_str(3.5), "medium");

    // High: 5.0 <= avg10 < 10.0
    let state = classify_pressure(5.0);
    assert!((state.memory_pressure - 0.7).abs() < f32::EPSILON);
    assert_eq!(pressure_level_str(5.0), "high");

    let state = classify_pressure(9.99);
    assert!((state.memory_pressure - 0.7).abs() < f32::EPSILON);
    assert_eq!(pressure_level_str(9.99), "high");

    // Critical: avg10 >= 10.0
    let state = classify_pressure(10.0);
    assert!((state.memory_pressure - 1.0).abs() < f32::EPSILON);
    assert_eq!(pressure_level_str(10.0), "critical");

    let state = classify_pressure(50.0);
    assert!((state.memory_pressure - 1.0).abs() < f32::EPSILON);
    assert_eq!(pressure_level_str(50.0), "critical");
}

// ─── Test: Simulated PSI is deterministic ─────────────────────────────────────

#[test]
fn test_simulated_psi_deterministic() {
    let clock = Arc::new(DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));

    let reader1 = SimulatedPsiReader::new(
        clock.clone(),
        EventEmitter::new(tokio::sync::mpsc::channel(64).0),
        42,
        None,
    );
    let reader2 = SimulatedPsiReader::new(
        clock.clone(),
        EventEmitter::new(tokio::sync::mpsc::channel(64).0),
        42,
        None,
    );

    // Same seed must produce identical values
    for resource in [PsiResource::Memory, PsiResource::Io, PsiResource::Cpu] {
        let s1 = reader1.read(resource).unwrap();
        let s2 = reader2.read(resource).unwrap();

        assert_eq!(s1.resource, s2.resource);
        assert!(
            (s1.avg10 - s2.avg10).abs() < f64::EPSILON,
            "avg10 mismatch for {:?}: {} vs {}",
            resource,
            s1.avg10,
            s2.avg10
        );
        assert!(
            (s1.avg60 - s2.avg60).abs() < f64::EPSILON,
            "avg60 mismatch for {:?}",
            resource
        );
        assert!(
            (s1.avg300 - s2.avg300).abs() < f64::EPSILON,
            "avg300 mismatch for {:?}",
            resource
        );
        assert_eq!(s1.total, s2.total);
    }

    // Different seeds must produce different values
    let reader3 = SimulatedPsiReader::new(
        clock,
        EventEmitter::new(tokio::sync::mpsc::channel(64).0),
        99,
        None,
    );
    let s1 = reader1.read(PsiResource::Memory).unwrap();
    let s3 = reader3.read(PsiResource::Memory).unwrap();
    assert_ne!(s1.avg10, s3.avg10, "Different seeds should produce different values");
}

// ─── Test: PSI reading emits events ────────────────────────────────────────────

#[test]
fn test_psi_emits_events() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let emitter = EventEmitter::new(tx);
    let clock = Arc::new(DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));

    let reader = SimulatedPsiReader::new(clock, emitter, 42, None);

    // Read memory — should emit MemoryPressureChanged
    let sample = reader.read(PsiResource::Memory).unwrap();
    let record = rx.try_recv().expect("should receive MemoryPressureChanged event");
    match record.event {
        Event::MemoryPressureChanged { avg10, avg60, avg300, total, .. } => {
            assert!((avg10 - sample.avg10).abs() < f64::EPSILON);
            assert!((avg60 - sample.avg60).abs() < f64::EPSILON);
            assert!((avg300 - sample.avg300).abs() < f64::EPSILON);
            assert_eq!(total, sample.total);
        }
        other => panic!("expected MemoryPressureChanged, got {:?}", other),
    }

    // Read I/O — should emit IoPressureChanged
    let sample = reader.read(PsiResource::Io).unwrap();
    let record = rx.try_recv().expect("should receive IoPressureChanged event");
    match record.event {
        Event::IoPressureChanged { avg10, avg60, avg300, total, .. } => {
            assert!((avg10 - sample.avg10).abs() < f64::EPSILON);
            assert!((avg60 - sample.avg60).abs() < f64::EPSILON);
            assert!((avg300 - sample.avg300).abs() < f64::EPSILON);
            assert_eq!(total, sample.total);
        }
        other => panic!("expected IoPressureChanged, got {:?}", other),
    }

    // Read CPU — should NOT emit a dedicated event (no CpuPressureChanged variant)
    let _sample = reader.read(PsiResource::Cpu).unwrap();
    assert!(rx.try_recv().is_err(), "CPU pressure should not emit an event");
}

// ─── Test: PSI replay produces identical stream ────────────────────────────────

#[test]
fn test_psi_replay() {
    let clock = Arc::new(DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));

    // Phase 1: Record
    let (tx1, mut rx1) = tokio::sync::mpsc::channel(64);
    let emitter1 = EventEmitter::new(tx1);
    let reader1 = SimulatedPsiReader::new(clock.clone(), emitter1, 42, None);

    let recorded_samples: Vec<_> = reader1
        .read_all()
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let recorded_events: Vec<Event> = std::iter::from_fn(|| rx1.try_recv().ok())
        .map(|r| r.event)
        .collect();

    // Phase 2: Replay with same seed
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
    let emitter2 = EventEmitter::new(tx2);
    let reader2 = SimulatedPsiReader::new(clock, emitter2, 42, None);

    let replayed_samples: Vec<_> = reader2
        .read_all()
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let replayed_events: Vec<Event> = std::iter::from_fn(|| rx2.try_recv().ok())
        .map(|r| r.event)
        .collect();

    // Verify identical samples
    assert_eq!(recorded_samples.len(), replayed_samples.len());
    for (orig, replayed) in recorded_samples.iter().zip(replayed_samples.iter()) {
        assert_eq!(orig.resource, replayed.resource);
        assert!((orig.avg10 - replayed.avg10).abs() < f64::EPSILON);
        assert!((orig.avg60 - replayed.avg60).abs() < f64::EPSILON);
        assert!((orig.avg300 - replayed.avg300).abs() < f64::EPSILON);
        assert_eq!(orig.total, replayed.total);
    }

    // Verify identical events
    assert_eq!(recorded_events.len(), replayed_events.len());
    for (orig, replayed) in recorded_events.iter().zip(replayed_events.iter()) {
        assert_eq!(orig.event_name(), replayed.event_name());
        assert_eq!(orig.category(), replayed.category());
    }
}

// ─── Test: Simulated PSI from file ────────────────────────────────────────────

#[test]
fn test_simulated_psi_from_file() {
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create a temporary PSI simulation file
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "memory: some avg10=3.14 avg60=2.71 avg300=1.41 total=5000").unwrap();
    let path = file.path().to_str().unwrap().to_string();

    let clock = Arc::new(DeterministicTimeProvider::new(1_700_000_000, std::time::Duration::from_secs(1)));
    let reader = SimulatedPsiReader::new(
        clock,
        EventEmitter::new(tokio::sync::mpsc::channel(64).0),
        42,
        Some(path),
    );

    let sample = reader.read(PsiResource::Memory).unwrap();
    assert!((sample.avg10 - 3.14).abs() < f64::EPSILON);
    assert!((sample.avg60 - 2.71).abs() < f64::EPSILON);
    assert!((sample.avg300 - 1.41).abs() < f64::EPSILON);
    assert_eq!(sample.total, 5000);
}
