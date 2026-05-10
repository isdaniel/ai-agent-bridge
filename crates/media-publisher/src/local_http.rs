//! In-process HTTP server that serves files registered via
//! [`LocalHttpPublisher::publish`].
//!
//! Files are registered under a UUID path: `/<base>/media/<uuid>`. The full
//! public URL is built from `public_base_url` (which the operator configures
//! to the externally-reachable URL of this server, possibly via a reverse
//! proxy or `ngrok`).
//!
//! Files are kept in an in-memory map only — they are NOT copied; the original
//! path must remain valid for as long as the publisher might serve it.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Path as AxumPath, State},
    http::{header, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use dashmap::DashMap;
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

use crate::MediaPublisher;

#[derive(Clone)]
struct Entry {
    path: PathBuf,
    mime: String,
}

#[derive(Clone)]
struct Inner {
    files: Arc<DashMap<String, Entry>>,
}

pub struct LocalHttpPublisher {
    inner: Inner,
    public_base_url: Url,
}

impl LocalHttpPublisher {
    /// Spawn the HTTP server bound to `bind` (e.g. `0.0.0.0:8081`) and return
    /// a publisher that serves files via `public_base_url` (e.g.
    /// `https://media.example.com`).
    pub async fn spawn(bind: SocketAddr, public_base_url: Url) -> anyhow::Result<Arc<Self>> {
        let inner = Inner {
            files: Arc::new(DashMap::new()),
        };
        let app = Router::new()
            .route("/media/{id}", get(serve))
            .with_state(inner.clone());
        let listener = tokio::net::TcpListener::bind(bind).await?;
        info!(%bind, %public_base_url, "media publisher listening");
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                warn!(error = %e, "media publisher terminated");
            }
        });
        Ok(Arc::new(Self {
            inner,
            public_base_url,
        }))
    }
}

async fn serve(
    AxumPath(id): AxumPath<String>,
    State(inner): State<Inner>,
) -> Result<Response, StatusCode> {
    let entry = inner.files.get(&id).ok_or(StatusCode::NOT_FOUND)?;
    let bytes = match tokio::fs::read(&entry.path).await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, path = ?entry.path, "media read failed");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, entry.mime.clone())
        .header(header::CONTENT_LENGTH, bytes.len().to_string())
        .body(Body::from(bytes))
        .unwrap())
}

#[async_trait]
impl MediaPublisher for LocalHttpPublisher {
    async fn publish(&self, path: &Path, mime: &str) -> anyhow::Result<Url> {
        let id = Uuid::new_v4().to_string();
        self.inner.files.insert(
            id.clone(),
            Entry {
                path: path.to_path_buf(),
                mime: mime.to_string(),
            },
        );
        let mut url = self.public_base_url.clone();
        let new_path = format!("{}/media/{}", url.path().trim_end_matches('/'), id);
        url.set_path(&new_path);
        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn publish_then_fetch_round_trip() {
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        // bind to ephemeral port → look up the actual addr after bind.
        let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
        let actual = listener.local_addr().unwrap();
        let inner = Inner {
            files: Arc::new(DashMap::new()),
        };
        let app = Router::new()
            .route("/media/{id}", get(serve))
            .with_state(inner.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let public = Url::parse(&format!("http://{actual}")).unwrap();
        let pub_ = LocalHttpPublisher {
            inner,
            public_base_url: public.clone(),
        };

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"hello-bytes").unwrap();
        let url = pub_.publish(tmp.path(), "text/plain").await.unwrap();
        assert!(url.path().starts_with("/media/"));

        let body = reqwest::get(url.as_str())
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "hello-bytes");
    }
}
