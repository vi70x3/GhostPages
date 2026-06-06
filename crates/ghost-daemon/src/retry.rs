//! Retry configuration with bounded exponential backoff.
//!
//! Provides configurable retry logic with jitter to avoid thundering herd
//! problems during recovery.

use std::time::Duration;

use ghost_core::error::GhostError;

/// SUBSYSTEM: Migration Engine
///
/// Configuration for retry behavior with bounded exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,

    /// Base delay before the first retry.
    pub base_delay_ms: u64,

    /// Maximum delay cap for exponential backoff.
    pub max_delay_ms: u64,

    /// Multiplier for exponential backoff.
    pub backoff_multiplier: f64,

    /// Jitter factor (0.0 = no jitter, 1.0 = full jitter).
    pub jitter_factor: f64,

    /// Error types that are eligible for retry.
    pub retryable_errors: Vec<String>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 30_000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.25,
            retryable_errors: vec![
                "io".to_string(),
                "timeout".to_string(),
                "backend_error".to_string(),
                "tier_unavailable".to_string(),
            ],
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration with custom parameters.
    pub fn new(max_retries: u32, base_delay_ms: u64, max_delay_ms: u64) -> Self {
        Self {
            max_retries,
            base_delay_ms,
            max_delay_ms,
            ..Default::default()
        }
    }

    /// Calculate the delay for a given retry attempt.
    ///
    /// Uses exponential backoff with jitter:
    /// delay = min(base_delay * multiplier^attempt, max_delay) * (1 ± jitter)
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(0);
        }

        // Calculate exponential backoff
        let base = self.base_delay_ms as f64;
        let multiplier = self.backoff_multiplier.powi((attempt - 1) as i32);
        let delay_ms = (base * multiplier).min(self.max_delay_ms as f64);

        // Apply jitter: random value in [delay * (1 - jitter), delay]
        let jitter_range = delay_ms * self.jitter_factor;
        let jittered = delay_ms - (jitter_range * random_fraction());

        Duration::from_millis(jittered.max(0.0) as u64)
    }

    /// Determine if an error is retryable based on its type.
    pub fn is_retryable(&self, error: &GhostError) -> bool {
        let error_type = error_type_tag(error);
        self.retryable_errors.contains(&error_type)
    }

    /// Check if the given attempt count is within the retry limit.
    pub fn has_retries_remaining(&self, attempt: u32) -> bool {
        attempt < self.max_retries
    }
}

/// Get a pseudo-random fraction in [0, 1) for jitter calculation.
///
/// Uses a simple xorshift-based approach to avoid pulling in `rand` as a
/// dependency for this single function.
fn random_fraction() -> f64 {
    // Use the current nanosecond timestamp as a simple entropy source
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    // Simple xorshift64 for better distribution
    let mut x = nanos as u64;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;

    (x as f64) / (u64::MAX as f64)
}

/// Map a GhostError to a string tag for retryability checking.
fn error_type_tag(error: &GhostError) -> String {
    match error {
        GhostError::Io(_) => "io".to_string(),
        GhostError::ChunkNotFound(_) => "chunk_not_found".to_string(),
        GhostError::TierFull(_) => "tier_full".to_string(),
        GhostError::TierUnavailable(_) => "tier_unavailable".to_string(),
        GhostError::ChecksumMismatch { .. } => "checksum_mismatch".to_string(),
        GhostError::CorruptionDetected { .. } => "corruption_detected".to_string(),
        GhostError::CompressionError(_) => "compression_error".to_string(),
        GhostError::BackendError(_) => "backend_error".to_string(),
        GhostError::IpcError(_) => "ipc_error".to_string(),
        GhostError::OutOfMemory => "out_of_memory".to_string(),
        GhostError::Timeout => "timeout".to_string(),
        GhostError::PipelineError(_) => "pipeline_error".to_string(),
        GhostError::Cancelled => "cancelled".to_string(),
        GhostError::ReplayError(_) => "replay_error".to_string(),
        GhostError::InvalidConfig(_) => "invalid_config".to_string(),
        GhostError::InvalidStateTransition { .. } => "invalid_state_transition".to_string(),
        GhostError::ProviderUnavailable(_) => "provider_unavailable".to_string(),
        GhostError::Internal(_) => "internal".to_string(),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 30_000);
        assert!((config.backoff_multiplier - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_delay_for_attempt_zero() {
        let config = RetryConfig::default();
        let delay = config.delay_for_attempt(0);
        assert_eq!(delay, Duration::from_millis(0));
    }

    #[test]
    fn test_delay_increases_with_attempts() {
        let config = RetryConfig {
            jitter_factor: 0.0, // No jitter for deterministic test
            ..Default::default()
        };

        let delay1 = config.delay_for_attempt(1);
        let delay2 = config.delay_for_attempt(2);
        let delay3 = config.delay_for_attempt(3);

        assert!(delay2 > delay1);
        assert!(delay3 > delay2);
    }

    #[test]
    fn test_delay_capped_at_max() {
        let config = RetryConfig {
            base_delay_ms: 1000,
            max_delay_ms: 2000,
            backoff_multiplier: 10.0,
            jitter_factor: 0.0,
            ..Default::default()
        };

        let delay = config.delay_for_attempt(10);
        assert_eq!(delay, Duration::from_millis(2000));
    }

    #[test]
    fn test_has_retries_remaining() {
        let config = RetryConfig {
            max_retries: 3,
            ..Default::default()
        };

        assert!(config.has_retries_remaining(0));
        assert!(config.has_retries_remaining(1));
        assert!(config.has_retries_remaining(2));
        assert!(!config.has_retries_remaining(3));
        assert!(!config.has_retries_remaining(4));
    }

    #[test]
    fn test_is_retryable() {
        let config = RetryConfig::default();

        assert!(config.is_retryable(&GhostError::Timeout));
        assert!(config.is_retryable(&GhostError::BackendError("test".to_string())));
        assert!(config.is_retryable(&GhostError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "test"
        ))));

        // Non-retryable errors
        assert!(!config.is_retryable(&GhostError::Cancelled));
        assert!(!config.is_retryable(&GhostError::InvalidConfig("test".to_string())));
    }

    #[test]
    fn test_jitter_applied() {
        let config = RetryConfig {
            base_delay_ms: 1000,
            jitter_factor: 0.5,
            ..Default::default()
        };

        // With jitter, delays should vary
        let delay = config.delay_for_attempt(1);
        // Delay should be in range [500, 1000] with 50% jitter
        assert!(delay >= Duration::from_millis(500));
        assert!(delay <= Duration::from_millis(1000));
    }

    #[test]
    fn test_custom_config() {
        let config = RetryConfig::new(5, 50, 10_000);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.base_delay_ms, 50);
        assert_eq!(config.max_delay_ms, 10_000);
    }
}
