//! Linux observation recorder.
//!
//! Records Linux system observations to a binary file for deterministic replay.
//! The binary format is compact and versioned:
//!
//! - Header: 4-byte magic (`GREC`), 1-byte version, 8-byte timestamp
//! - Events: length-prefixed bincode-serialized `EventRecord`s
//! - Footer: 4-byte end marker (`GEND`)

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ghost_core::error::{GhostError, GhostResult};
use ghost_core::events::EventRecord;

use crate::meminfo::MeminfoSnapshot;
use crate::psi::PsiSample;
use crate::swaps::SwapTopology;
use crate::tier_inventory::TierInfo;
use crate::vmstat::VmstatSnapshot;
use crate::zram::ZramSnapshot;

/// Binary format version.
pub(crate) const RECORD_FORMAT_VERSION: u8 = 1;

/// Magic bytes for file header.
pub(crate) const MAGIC_HEADER: &[u8] = b"GREC";

/// Magic bytes for file footer.
pub(crate) const MAGIC_END: &[u8] = b"GEND";

/// A complete Linux system snapshot for recording.
///
/// Captures all observation layers at a single point in time.
#[derive(Debug, Clone)]
pub struct LinuxSnapshot {
    /// Timestamp when the snapshot was taken (seconds since epoch).
    pub timestamp: u64,

    /// PSI pressure samples (memory, I/O, CPU).
    pub psi: Option<Vec<PsiSample>>,

    /// Memory statistics from `/proc/meminfo`.
    pub meminfo: Option<MeminfoSnapshot>,

    /// VM statistics from `/proc/vmstat`.
    pub vmstat: Option<VmstatSnapshot>,

    /// Swap topology from `/proc/swaps`.
    pub swap: Option<SwapTopology>,

    /// ZRAM device statistics.
    pub zram: Option<ZramSnapshot>,

    /// Current tier inventory.
    pub tier_inventory: Option<Vec<TierInfo>>,

    /// Policy recommendations based on current state.
    pub recommendations: Vec<String>,
}

impl LinuxSnapshot {
    /// Serialize the snapshot into an EventRecord with the given sequence ID.
    pub fn to_event_record(&self, sequence_id: u64) -> EventRecord {
        let payload = serde_json::json!({
            "timestamp": self.timestamp,
            "psi": self.psi,
            "meminfo": self.meminfo,
            "vmstat": self.vmstat,
            "swap": self.swap,
            "zram": self.zram,
            "tier_inventory": self.tier_inventory,
            "recommendations": self.recommendations,
        });

        EventRecord {
            sequence_id,
            timestamp: self.timestamp,
            event: ghost_core::events::Event::PolicyRecommendationGenerated {
                sequence_id,
                recommendations: vec![payload.to_string()],
                pressure_level: "snapshot".to_string(),
            },
        }
    }
}

/// Records Linux observations to a binary file.
pub struct LinuxRecorder {
    writer: BufWriter<File>,
    path: PathBuf,
    sequence: AtomicU64,
}

impl LinuxRecorder {
    /// Create a new recorder that writes to the given file path.
    pub fn new(path: &Path) -> GhostResult<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(GhostError::Io)?;

        let mut writer = BufWriter::new(file);

        // Write header: magic + version + timestamp
        writer.write_all(MAGIC_HEADER).map_err(GhostError::Io)?;
        writer.write_all(&[RECORD_FORMAT_VERSION])
            .map_err(GhostError::Io)?;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        writer
            .write_all(&timestamp.to_le_bytes())
            .map_err(GhostError::Io)?;

        writer.flush().map_err(GhostError::Io)?;

        Ok(Self {
            writer,
            path: path.to_path_buf(),
            sequence: AtomicU64::new(1),
        })
    }

    /// Record a single Linux observation event.
    pub fn record(&mut self, event: &EventRecord) -> GhostResult<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let mut record = event.clone();
        record.sequence_id = seq;

        self.write_record(&record)
    }

    /// Record a batch of observations from a full system scan.
    pub fn record_scan(&mut self, snapshot: &LinuxSnapshot) -> GhostResult<()> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let record = snapshot.to_event_record(seq);
        self.write_record(&record)
    }

    /// Write a single EventRecord to the file.
    fn write_record(&mut self, record: &EventRecord) -> GhostResult<()> {
        let bytes = bincode::serialize(record).map_err(|e| {
            GhostError::Internal(format!("failed to serialize event record: {}", e))
        })?;

        let len = bytes.len() as u32;
        self.writer
            .write_all(&len.to_le_bytes())
            .map_err(GhostError::Io)?;
        self.writer.write_all(&bytes).map_err(GhostError::Io)?;
        self.writer.flush().map_err(GhostError::Io)?;

        Ok(())
    }

    /// Flush and close the recorder, writing the end marker.
    pub fn close(mut self) -> GhostResult<()> {
        self.writer.write_all(MAGIC_END).map_err(GhostError::Io)?;
        self.writer.flush().map_err(GhostError::Io)?;
        Ok(())
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the current sequence number.
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }
}

impl Drop for LinuxRecorder {
    fn drop(&mut self) {
        let _ = self.writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ghost_core::events::Event;
    use tempfile::NamedTempFile;

    #[test]
    fn test_recorder_creates_file() {
        let tmp = NamedTempFile::new().unwrap();
        let recorder = LinuxRecorder::new(tmp.path()).unwrap();
        assert!(tmp.path().exists());
        let _ = recorder.close();
    }

    #[test]
    fn test_recorder_writes_event() {
        let tmp = NamedTempFile::new().unwrap();
        let mut recorder = LinuxRecorder::new(tmp.path()).unwrap();

        let event = EventRecord {
            sequence_id: 0,
            timestamp: 1_700_000_000,
            event: Event::MemoryStatsChanged {
                sequence_id: 0,
                total_kb: 16_000_000,
                available_kb: 8_000_000,
                swap_used_kb: 1_000_000,
                dirty_kb: 100_000,
            },
        };

        recorder.record(&event).unwrap();
        let _ = recorder.close();

        let metadata = std::fs::metadata(tmp.path()).unwrap();
        assert!(metadata.len() > 0);
    }

    #[test]
    fn test_recorder_sequence_increments() {
        let tmp = NamedTempFile::new().unwrap();
        let mut recorder = LinuxRecorder::new(tmp.path()).unwrap();

        assert_eq!(recorder.current_sequence(), 1);

        let event = EventRecord {
            sequence_id: 0,
            timestamp: 1_700_000_000,
            event: Event::MemoryStatsChanged {
                sequence_id: 0,
                total_kb: 16_000_000,
                available_kb: 8_000_000,
                swap_used_kb: 1_000_000,
                dirty_kb: 100_000,
            },
        };

        recorder.record(&event).unwrap();
        assert_eq!(recorder.current_sequence(), 2);

        recorder.record(&event).unwrap();
        assert_eq!(recorder.current_sequence(), 3);

        let _ = recorder.close();
    }

    #[test]
    fn test_recorder_record_scan() {
        let tmp = NamedTempFile::new().unwrap();
        let mut recorder = LinuxRecorder::new(tmp.path()).unwrap();

        let snapshot = LinuxSnapshot {
            timestamp: 1_700_000_000,
            psi: None,
            meminfo: None,
            vmstat: None,
            swap: None,
            zram: None,
            tier_inventory: None,
            recommendations: vec!["no_action".to_string()],
        };

        recorder.record_scan(&snapshot).unwrap();
        let _ = recorder.close();

        let metadata = std::fs::metadata(tmp.path()).unwrap();
        assert!(metadata.len() > 0);
    }
}
