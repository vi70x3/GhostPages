//! Tracing setup module.

use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize the tracing subscriber with env-filter support.
///
/// This sets up structured logging with the following features:
/// - Environment variable filtering (e.g., `RUST_LOG=ghost_pages=debug`)
/// - Timestamps on log lines
/// - Target and level information
/// - Span events
///
/// # Examples
///
/// ```
/// use ghost_metrics::tracing::init_tracing;
///
/// // Initialize with default settings
/// init_tracing();
///
/// // Now tracing macros work:
/// tracing::info!("GhostPages starting up");
/// tracing::debug!(target: "ghost_daemon", "Daemon initialized");
/// ```
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();

    tracing::info!("Tracing initialized");
}

/// Initialize tracing with a custom filter string.
///
/// # Examples
///
/// ```
/// use ghost_metrics::tracing::init_tracing_with_filter;
///
/// // Initialize with custom filter
/// init_tracing_with_filter("ghost_core=debug,ghost_daemon=info");
/// ```
pub fn init_tracing_with_filter(filter_str: &str) {
    let filter = EnvFilter::new(filter_str);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();

    tracing::info!(
        filter = filter_str,
        "Tracing initialized with custom filter"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: tracing can only be initialized once per process,
    // so we can't test init_tracing() in unit tests.
    // Integration tests should verify tracing output.

    #[test]
    fn test_env_filter_parsing() {
        // Just verify that EnvFilter can be created from a string
        let filter = EnvFilter::new("debug");
        // We can't easily test the filter without initializing tracing,
        // but we can at least verify it doesn't panic
        let _ = filter;
    }
}
