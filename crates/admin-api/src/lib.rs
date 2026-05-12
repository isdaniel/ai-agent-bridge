//! Lightweight admin/observability API server.
//!
//! Endpoints:
//! - `GET /healthz` — 200 OK (liveness probe)
//! - `GET /api/metrics` — JSON engine stats
//! - `GET /api/sessions` — JSON list of active sessions

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use core_engine::Engine;
use tracing::info;

pub struct AdminServer {
    bind: String,
    engine: Arc<Engine>,
}

impl AdminServer {
    pub fn new(bind: impl Into<String>, engine: Arc<Engine>) -> Self {
        Self {
            bind: bind.into(),
            engine,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let app = Router::new()
            .route("/healthz", get(healthz))
            .route("/api/metrics", get(metrics))
            .route("/api/sessions", get(sessions))
            .with_state(self.engine);

        info!(bind=%self.bind, "admin API listening");
        let listener = tokio::net::TcpListener::bind(&self.bind).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn metrics(State(engine): State<Arc<Engine>>) -> impl IntoResponse {
    let stats = engine.stats();
    Json(serde_json::json!({
        "active_sessions": stats.active_sessions,
        "agents": stats.agents,
        "platforms": stats.platforms,
        "default_agent": stats.default_agent,
        "uptime_s": stats.uptime_s,
        "max_sessions": stats.max_sessions,
    }))
}

async fn sessions(State(engine): State<Arc<Engine>>) -> impl IntoResponse {
    let stats = engine.stats();
    Json(stats.sessions)
}
