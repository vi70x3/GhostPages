//! Migration metrics for the migration engine.

use prometheus::{IntCounter, IntGauge, Registry};

/// Metrics for the migration engine.
#[derive(Debug, Clone)]
pub struct MigrationMetrics {
    /// Total number of evaluation cycles.
    pub evaluation_cycles_total: IntCounter,
    /// Total number of promotions performed.
    pub promotions_total: IntCounter,
    /// Total number of evictions performed.
    pub evictions_total: IntCounter,
    /// Total number of skipped evaluations.
    pub skipped_total: IntCounter,
    /// Total number of migration failures.
    pub failures_total: IntCounter,
    /// Total bytes migrated.
    pub bytes_migrated_total: IntCounter,
    /// Currently active migrations.
    pub active_migrations: IntGauge,
    /// Total number of pending migrations identified.
    pub pending_identified_total: IntCounter,
}

impl MigrationMetrics {
    /// Create a new MigrationMetrics instance and register with the given registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let evaluation_cycles_total = IntCounter::new(
            "ghostpages_migration_evaluation_cycles_total",
            "Total number of evaluation cycles",
        )?;
        let promotions_total = IntCounter::new(
            "ghostpages_migration_promotions_total",
            "Total number of promotions performed",
        )?;
        let evictions_total = IntCounter::new(
            "ghostpages_migration_evictions_total",
            "Total number of evictions performed",
        )?;
        let skipped_total = IntCounter::new(
            "ghostpages_migration_skipped_total",
            "Total number of skipped evaluations",
        )?;
        let failures_total = IntCounter::new(
            "ghostpages_migration_failures_total",
            "Total number of migration failures",
        )?;
        let bytes_migrated_total = IntCounter::new(
            "ghostpages_migration_bytes_migrated_total",
            "Total bytes migrated",
        )?;
        let active_migrations = IntGauge::new(
            "ghostpages_migration_active",
            "Currently active migrations",
        )?;
        let pending_identified_total = IntCounter::new(
            "ghostpages_migration_pending_identified_total",
            "Total number of pending migrations identified",
        )?;

        registry.register(Box::new(evaluation_cycles_total.clone()))?;
        registry.register(Box::new(promotions_total.clone()))?;
        registry.register(Box::new(evictions_total.clone()))?;
        registry.register(Box::new(skipped_total.clone()))?;
        registry.register(Box::new(failures_total.clone()))?;
        registry.register(Box::new(bytes_migrated_total.clone()))?;
        registry.register(Box::new(active_migrations.clone()))?;
        registry.register(Box::new(pending_identified_total.clone()))?;

        Ok(Self {
            evaluation_cycles_total,
            promotions_total,
            evictions_total,
            skipped_total,
            failures_total,
            bytes_migrated_total,
            active_migrations,
            pending_identified_total,
        })
    }
}
