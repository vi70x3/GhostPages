//! Trace export for GhostPages.
//!
//! Exports trace events to JSON, CSV, and JSON Lines formats.

use std::io::Write;
use std::path::Path;

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::trace::TraceEvent;

/// Supported export formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// Pretty-printed JSON array.
    Json,
    /// JSON Lines (one JSON object per line).
    JsonLines,
    /// Comma-separated values with header row.
    Csv,
}

impl std::str::FromStr for ExportFormat {
    type Err = GhostError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "jsonl" | "jsonlines" => Ok(Self::JsonLines),
            "csv" => Ok(Self::Csv),
            other => Err(GhostError::ReplayError(format!(
                "unknown export format: {}",
                other
            ))),
        }
    }
}

impl std::fmt::Display for ExportFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => write!(f, "json"),
            Self::JsonLines => write!(f, "jsonl"),
            Self::Csv => write!(f, "csv"),
        }
    }
}

/// Export trace events to a file in the given format.
pub fn export_trace<W: Write>(
    events: &[TraceEvent],
    writer: &mut W,
    format: ExportFormat,
) -> GhostResult<()> {
    match format {
        ExportFormat::Json => export_json(events, writer),
        ExportFormat::JsonLines => export_jsonl(events, writer),
        ExportFormat::Csv => export_csv(events, writer),
    }
}

/// Export trace events to a file path in the given format.
pub async fn export_trace_to_file(
    events: &[TraceEvent],
    path: &Path,
    format: ExportFormat,
) -> GhostResult<()> {
    let mut file = std::fs::File::create(path)
        .map_err(|e| GhostError::ReplayError(format!("failed to create export file: {}", e)))?;
    export_trace(events, &mut file, format)
}

// ─── JSON ─────────────────────────────────────────────────────────────────────

fn export_json<W: Write>(events: &[TraceEvent], writer: &mut W) -> GhostResult<()> {
    serde_json::to_writer_pretty(writer, events)
        .map_err(|e| GhostError::ReplayError(format!("failed to write JSON: {}", e)))
}

// ─── JSON Lines ───────────────────────────────────────────────────────────────

fn export_jsonl<W: Write>(events: &[TraceEvent], writer: &mut W) -> GhostResult<()> {
    for event in events {
        let line = serde_json::to_vec(event)
            .map_err(|e| GhostError::ReplayError(format!("failed to serialize event: {}", e)))?;
        writer.write_all(&line)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

// ─── CSV ──────────────────────────────────────────────────────────────────────

fn export_csv<W: Write>(events: &[TraceEvent], writer: &mut W) -> GhostResult<()> {
    // Header row
    writeln!(writer, "timestamp,event_type,chunk_id,tier,extra")
        .map_err(|e| GhostError::ReplayError(format!("failed to write CSV header: {}", e)))?;

    for event in events {
        let timestamp = event.timestamp();
        let event_type = event.event_type();
        let chunk_id = event
            .chunk_id()
            .map(|id| id.short_hex())
            .unwrap_or_default();
        let tier = event_tier(event);
        let extra = event_extra(event);

        writeln!(
            writer,
            "{},{},{},{},{}",
            timestamp,
            csv_escape(event_type),
            chunk_id,
            csv_escape(&tier),
            csv_escape(&extra),
        )
        .map_err(|e| GhostError::ReplayError(format!("failed to write CSV row: {}", e)))?;
    }

    Ok(())
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn event_tier(event: &TraceEvent) -> String {
    match event {
        TraceEvent::ChunkCreated { tier, .. } => format!("{}", tier),
        TraceEvent::ChunkDeleted { tier, .. } => format!("{}", tier),
        TraceEvent::Eviction { tier, .. } => format!("{}", tier),
        TraceEvent::TransferQueued { from, to, .. } => format!("{}->{}", from, to),
        TraceEvent::TransferStarted { job, .. } => format!("{}->{}", job.from_tier, job.to_tier),
        TraceEvent::TransferCompleted { from, to, .. } => format!("{}->{}", from, to),
        TraceEvent::TransferFailed { from, to, .. } => format!("{}->{}", from, to),
        TraceEvent::TransferRetry { from, to, .. } => format!("{}->{}", from, to),
        TraceEvent::TransferCancelled { from, to, .. } => format!("{}->{}", from, to),
        TraceEvent::PolicyDecision { from, to, .. } => format!("{}->{}", from, to),
        TraceEvent::BackendRegistered { tier, .. } => format!("{}", tier),
        _ => String::new(),
    }
}

fn event_extra(event: &TraceEvent) -> String {
    match event {
        TraceEvent::ChunkCreated { size, .. } => format!("size={}", size),
        TraceEvent::ChunkStateChanged { from, to, .. } => format!("{}->{}", from, to),
        TraceEvent::Eviction { reason, .. } => format!("reason={:?}", reason),
        TraceEvent::TransferCompleted { duration_ms, .. } => format!("duration_ms={}", duration_ms),
        TraceEvent::TransferFailed { error, attempt, .. } => {
            format!("error={},attempt={}", error, attempt)
        }
        TraceEvent::PressureAlert {
            memory_pressure,
            vram_pressure,
            io_pressure,
            ..
        } => {
            format!(
                "mem={},vram={},io={}",
                memory_pressure, vram_pressure, io_pressure
            )
        }
        TraceEvent::PolicyDecision { reason, .. } => format!("reason={}", reason),
        TraceEvent::CompressionCompleted {
            original_size,
            compressed_size,
            ..
        } => {
            format!("orig={},compressed={}", original_size, compressed_size)
        }
        _ => String::new(),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::state::ChunkState;
    use ghost_core::types::{ChunkId, TierId};

    fn test_events() -> Vec<TraceEvent> {
        vec![
            TraceEvent::ChunkCreated {
                chunk_id: ChunkId::from_data(b"chunk1"),
                size: 1024,
                tier: TierId::Ram,
                timestamp: 1000,
            },
            TraceEvent::ChunkStateChanged {
                chunk_id: ChunkId::from_data(b"chunk1"),
                from: ChunkState::Allocated,
                to: ChunkState::Stored,
                timestamp: 1001,
            },
            TraceEvent::Eviction {
                chunk_id: ChunkId::from_data(b"chunk1"),
                tier: TierId::Ram,
                reason: ghost_core::trace::EvictionReason::Capacity,
                timestamp: 1002,
            },
        ]
    }

    #[test]
    fn test_export_json() {
        let events = test_events();
        let mut buf = Vec::new();
        export_trace(&events, &mut buf, ExportFormat::Json).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("ChunkCreated"));
        assert!(output.contains("ChunkStateChanged"));
        assert!(output.contains("Eviction"));

        // Verify it's valid JSON
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 3);
    }

    #[test]
    fn test_export_jsonl() {
        let events = test_events();
        let mut buf = Vec::new();
        export_trace(&events, &mut buf, ExportFormat::JsonLines).unwrap();

        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 3);

        // Each line should be valid JSON
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
    }

    #[test]
    fn test_export_csv() {
        let events = test_events();
        let mut buf = Vec::new();
        export_trace(&events, &mut buf, ExportFormat::Csv).unwrap();

        let output = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 4); // header + 3 events
        assert!(lines[0].contains("timestamp"));
        assert!(lines[0].contains("event_type"));
    }

    #[test]
    fn test_csv_escape() {
        assert_eq!(csv_escape("hello"), "hello");
        assert_eq!(csv_escape("hello,world"), "\"hello,world\"");
        assert_eq!(csv_escape("hello\"world"), "\"hello\"\"world\"");
    }

    #[test]
    fn test_export_format_from_str() {
        assert_eq!("json".parse::<ExportFormat>().unwrap(), ExportFormat::Json);
        assert_eq!(
            "jsonl".parse::<ExportFormat>().unwrap(),
            ExportFormat::JsonLines
        );
        assert_eq!(
            "jsonlines".parse::<ExportFormat>().unwrap(),
            ExportFormat::JsonLines
        );
        assert_eq!("csv".parse::<ExportFormat>().unwrap(), ExportFormat::Csv);
        assert!("xml".parse::<ExportFormat>().is_err());
    }

    #[test]
    fn test_export_format_display() {
        assert_eq!(format!("{}", ExportFormat::Json), "json");
        assert_eq!(format!("{}", ExportFormat::JsonLines), "jsonl");
        assert_eq!(format!("{}", ExportFormat::Csv), "csv");
    }
}
