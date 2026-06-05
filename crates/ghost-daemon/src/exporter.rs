//! Prometheus metrics HTTP exporter for the GhostPages daemon.
//!
//! Provides an HTTP endpoint for Prometheus scraping, serving metrics
//! in the standard Prometheus text format. The exporter binds to
//! `127.0.0.1:9090` by default and exposes `/metrics` for scraping
//! and `/health` for health checks.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Json;
use axum::Router;
use ghost_core::error::GhostResult;
use prometheus::{Encoder, Registry, TextEncoder};
use serde::Serialize;
use tokio::net::TcpListener;

use crate::config::MetricsExporterConfig;

/// Metrics exporter state shared across HTTP handlers.
#[derive(Clone)]
pub struct ExporterState {
    /// The Prometheus registry containing all metrics.
    pub registry: Arc<Registry>,
}

/// Prometheus metrics HTTP exporter.
pub struct MetricsExporter {
    config: MetricsExporterConfig,
    state: ExporterState,
}

impl MetricsExporter {
    /// Create a new metrics exporter with the given configuration and registry.
    pub fn new(config: MetricsExporterConfig, registry: Arc<Registry>) -> Self {
        Self {
            config,
            state: ExporterState { registry },
        }
    }

    /// Run the metrics exporter, listening for HTTP requests.
    ///
    /// This method blocks until the exporter is shut down.
    pub async fn run(&self) -> GhostResult<()> {
        let addr: SocketAddr = format!("{}:{}", self.config.bind_address, self.config.port)
            .parse()
            .map_err(|e| {
                ghost_core::error::GhostError::Internal(format!(
                    "invalid metrics exporter bind address: {}",
                    e
                ))
            })?;

        let state = self.state.clone();

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .route("/", get(root_handler))
            .with_state(state);

        tracing::info!(
            "Metrics exporter listening on http://{}:{}/metrics",
            self.config.bind_address,
            self.config.port
        );

        let listener = TcpListener::bind(addr).await.map_err(|e| {
            ghost_core::error::GhostError::Internal(format!(
                "failed to bind metrics exporter to {}: {}",
                addr, e
            ))
        })?;

        axum::serve(listener, app)
            .await
            .map_err(|e| ghost_core::error::GhostError::Internal(format!(
                "metrics exporter server error: {}",
                e
            )))?;

        Ok(())
    }
}

/// Handler for the `/metrics` endpoint.
///
/// Serves all registered Prometheus metrics in text format.
async fn metrics_handler(State(state): State<ExporterState>) -> Response {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = state.registry.gather();

    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!("failed to encode metrics: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {}", e),
        )
            .into_response();
    }

    let body = String::from_utf8_lossy(&buffer).to_string();
    (StatusCode::OK, body).into_response()
}

/// Handler for the `/health` endpoint.
///
/// Returns a simple JSON health check response.
async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        service: "ghostpages-metrics".to_string(),
    })
}

/// Handler for the root `/` endpoint.
///
/// Returns a simple HTML page with links to metrics and health endpoints.
async fn root_handler() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html>
<head><title>GhostPages Metrics Exporter</title></head>
<body>
<h1>GhostPages Metrics Exporter</h1>
<ul>
<li><a href="/metrics">/metrics</a> — Prometheus metrics</li>
<li><a href="/health">/health</a> — Health check</li>
</ul>
</body>
</html>"#,
    )
}

/// Health check response.
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    service: String,
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{IntCounter, Registry};

    #[test]
    fn test_metrics_exporter_creation() {
        let registry = Arc::new(Registry::new());
        let config = MetricsExporterConfig::default();
        let exporter = MetricsExporter::new(config, registry);
        // Just verify it was created successfully
        let _ = exporter;
    }

    #[test]
    fn test_exporter_state_clone() {
        let registry = Arc::new(Registry::new());
        let state = ExporterState { registry };
        let _cloned = state.clone();
    }

    #[tokio::test]
    async fn test_metrics_handler() {
        let registry = Arc::new(Registry::new());
        let counter = IntCounter::new("test_counter", "a test counter").unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        counter.inc();

        let state = ExporterState {
            registry: registry.clone(),
        };

        let response = metrics_handler(State(state)).await;
        // axum response status check
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_handler() {
        let response = health_handler().await;
        assert_eq!(response.0.status, "ok");
    }
}
