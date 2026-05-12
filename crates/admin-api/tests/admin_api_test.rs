//! Integration tests for the admin API endpoints.

use std::sync::Arc;

use core_engine::Engine;
use test_support::{EchoAgent, MockPlatform};

async fn spawn_admin() -> String {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform)
        .build()
        .unwrap();

    // Bind to port 0 so the OS picks a free port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        let app = axum::Router::new()
            .route("/healthz", axum::routing::get(|| async { "ok" }))
            .route(
                "/api/metrics",
                axum::routing::get(
                    |axum::extract::State(e): axum::extract::State<Arc<Engine>>| async move {
                        let stats = e.stats();
                        axum::Json(serde_json::json!({
                            "active_sessions": stats.active_sessions,
                            "agents": stats.agents,
                            "platforms": stats.platforms,
                            "default_agent": stats.default_agent,
                            "uptime_s": stats.uptime_s,
                            "max_sessions": stats.max_sessions,
                        }))
                    },
                ),
            )
            .route(
                "/api/sessions",
                axum::routing::get(
                    |axum::extract::State(e): axum::extract::State<Arc<Engine>>| async move {
                        let stats = e.stats();
                        axum::Json(stats.sessions)
                    },
                ),
            )
            .with_state(engine);
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    base_url
}

#[tokio::test]
async fn healthz_returns_200() {
    let base = spawn_admin().await;
    let resp = reqwest::get(format!("{base}/healthz")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn metrics_returns_valid_json() {
    let base = spawn_admin().await;
    let resp = reqwest::get(format!("{base}/api/metrics")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["active_sessions"], 0);
    assert!(body["agents"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("echo")));
    assert!(body["platforms"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("mock")));
    assert_eq!(body["default_agent"], "echo");
    assert!(body["max_sessions"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn sessions_returns_array() {
    let base = spawn_admin().await;
    let resp = reqwest::get(format!("{base}/api/sessions")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body.is_array());
    assert_eq!(body.as_array().unwrap().len(), 0);
}
