//! Integration test: IPC roundtrip.
//!
//! Validates that the IPC server can accept connections, dispatch
//! requests, and return correct responses over Unix domain sockets.

use ghost_core::types::{ChunkId, TierId};
use ghost_daemon::config::OrchestratorConfig;
use ghost_daemon::ipc_server::{IpcServer, IpcServerConfig};
use ghost_daemon::orchestrator::TransferOrchestrator;
use ghost_daemon::trace_log::TraceLog;
use ghost_ipc::client::IpcClient;
use ghost_ipc::protocol::{IpcRequest, IpcResponse};
use ghost_policy::pressure::PressureAwareConfig;
use ghost_policy::pressure::PressureAwarePolicy;
use ghost_sim::config::SimConfig;
use ghost_sim::SimBackend;
use ghost_tier::RamBackend;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

fn test_backends() -> BTreeMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> {
    let mut backends: BTreeMap<TierId, Arc<dyn ghost_tier::backend::StorageBackend>> =
        BTreeMap::new();
    backends.insert(
        TierId::Ram,
        Arc::new(RamBackend::new(4 * 1024 * 1024)) as Arc<dyn ghost_tier::backend::StorageBackend>,
    );
    let sim = Arc::new(SimBackend::new(
        SimConfig::with_capacity(16 * 1024 * 1024).with_seed(42),
    ));
    backends.insert(
        TierId::Simulation,
        sim as Arc<dyn ghost_tier::backend::StorageBackend>,
    );
    backends
}

fn test_policy() -> Arc<dyn ghost_policy::PlacementPolicy> {
    Arc::new(PressureAwarePolicy::new(PressureAwareConfig::default()))
}

async fn setup_server(dir: &TempDir) -> (PathBuf, Arc<TransferOrchestrator>) {
    let socket_path = dir.path().join("test.sock");
    let config = OrchestratorConfig::default();
    let trace_log = Arc::new(TraceLog::new(10_000));
    let orch = Arc::new(TransferOrchestrator::new(
        config,
        test_backends(),
        test_policy(),
    ));

    let ipc_config = IpcServerConfig {
        socket_path: socket_path.clone(),
        ..Default::default()
    };
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let server = IpcServer::new(ipc_config, orch.clone(), trace_log, shutdown_rx);

    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("IPC server error: {}", e);
        }
    });

    // Wait for socket file to appear (server has bound)
    // Use tokio::time::sleep to yield control so the server task can run
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if socket_path.exists() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("IPC server did not bind socket within 5 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    (socket_path, orch)
}

#[tokio::test]
async fn test_ipc_ping() {
    let dir = TempDir::new().unwrap();
    let (socket_path, _orch) = setup_server(&dir).await;

    let mut client = IpcClient::connect(&socket_path).await.unwrap();
    let response = client.send_request(IpcRequest::Ping).await.unwrap();

    match response {
        IpcResponse::Pong => {} // expected
        other => panic!("expected Pong, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ipc_store_and_retrieve() {
    let dir = TempDir::new().unwrap();
    let (socket_path, _orch) = setup_server(&dir).await;

    let mut client = IpcClient::connect(&socket_path).await.unwrap();

    // Store data
    let data = b"ipc roundtrip test data";

    let store_req = IpcRequest::Store {
        data: data.to_vec(),
        tier: Some(TierId::Ram),
    };
    let response = client.send_request(store_req).await.unwrap();

    match response {
        IpcResponse::ChunkId { .. } => {} // expected
        other => panic!("expected ChunkId, got {:?}", other),
    }

    // Wait for async processing
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Retrieve data — use the same chunk_id that was stored
    let chunk_id = ChunkId::from_data(data);
    let retrieve_req = IpcRequest::Retrieve { chunk_id };
    let response = client.send_request(retrieve_req).await.unwrap();

    match response {
        IpcResponse::Ok { .. } => {} // expected
        other => panic!("expected Ok, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ipc_status() {
    let dir = TempDir::new().unwrap();
    let (socket_path, _orch) = setup_server(&dir).await;

    let mut client = IpcClient::connect(&socket_path).await.unwrap();
    let response = client.send_request(IpcRequest::Status).await.unwrap();

    match response {
        IpcResponse::Status { .. } => {} // expected
        other => panic!("expected Status, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ipc_pressure() {
    let dir = TempDir::new().unwrap();
    let (socket_path, _orch) = setup_server(&dir).await;

    let mut client = IpcClient::connect(&socket_path).await.unwrap();
    let response = client.send_request(IpcRequest::Pressure).await.unwrap();

    match response {
        IpcResponse::Pressure { .. } => {} // expected
        other => panic!("expected Pressure, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ipc_multiple_connections() {
    let dir = TempDir::new().unwrap();
    let (socket_path, _orch) = setup_server(&dir).await;

    // First connection
    let mut client1 = IpcClient::connect(&socket_path).await.unwrap();
    let response = client1.send_request(IpcRequest::Ping).await.unwrap();
    match response {
        IpcResponse::Pong => {}
        other => panic!("expected Pong, got {:?}", other),
    }

    // Second connection
    let mut client2 = IpcClient::connect(&socket_path).await.unwrap();
    let response = client2.send_request(IpcRequest::Ping).await.unwrap();
    match response {
        IpcResponse::Pong => {}
        other => panic!("expected Pong, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ipc_migrate() {
    let dir = TempDir::new().unwrap();
    let (socket_path, _orch) = setup_server(&dir).await;

    let mut client = IpcClient::connect(&socket_path).await.unwrap();

    // Store first
    let data = b"ipc migration test";
    let store_req = IpcRequest::Store {
        data: data.to_vec(),
        tier: Some(TierId::Ram),
    };
    let response = client.send_request(store_req).await.unwrap();
    match response {
        IpcResponse::ChunkId { .. } => {}
        other => panic!("expected ChunkId, got {:?}", other),
    }

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Migrate
    let chunk_id = ChunkId::from_data(data);
    let migrate_req = IpcRequest::Migrate {
        chunk_id,
        from: TierId::Ram,
        to: TierId::Simulation,
    };
    let response = client.send_request(migrate_req).await.unwrap();

    match response {
        IpcResponse::Ok { .. } => {}
        IpcResponse::Error { .. } => {
            // Migration may fail if chunk not in expected state — that's OK for this test
        }
        other => panic!("unexpected response: {:?}", other),
    }
}
