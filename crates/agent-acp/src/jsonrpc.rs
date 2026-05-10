//! Generic JSON-RPC 2.0 client over async byte streams.
//!
//! - Outgoing requests get an auto-incrementing id and a `oneshot` channel
//!   in [`PendingMap`]; the reply task resolves them.
//! - Outgoing notifications carry no id; no entry is registered.
//! - Incoming notifications are routed to a per-method `mpsc::Sender` if one
//!   is registered; otherwise dropped with a warning.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, warn};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Default)]
pub struct PendingMap {
    inner: DashMap<i64, oneshot::Sender<Result<serde_json::Value, JsonRpcError>>>,
}

impl PendingMap {
    pub fn insert(&self, id: i64, tx: oneshot::Sender<Result<serde_json::Value, JsonRpcError>>) {
        self.inner.insert(id, tx);
    }
    pub fn resolve(&self, id: i64, value: Result<serde_json::Value, JsonRpcError>) {
        if let Some((_, tx)) = self.inner.remove(&id) {
            let _ = tx.send(value);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

impl std::fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JSON-RPC {}: {}", self.code, self.message)
    }
}

impl std::error::Error for JsonRpcError {}

#[derive(Debug, Deserialize)]
struct Frame {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    params: Option<serde_json::Value>,
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<JsonRpcError>,
}

pub struct JsonRpcClient<W: AsyncWrite + Unpin + Send + 'static> {
    next_id: AtomicI64,
    pending: Arc<PendingMap>,
    notify_routes: Arc<DashMap<String, mpsc::Sender<serde_json::Value>>>,
    writer: Arc<Mutex<W>>,
}

impl<W: AsyncWrite + Unpin + Send + 'static> JsonRpcClient<W> {
    pub fn new(writer: W) -> Self {
        Self {
            next_id: AtomicI64::new(1),
            pending: Arc::new(PendingMap::default()),
            notify_routes: Arc::new(DashMap::new()),
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    pub fn pending(&self) -> Arc<PendingMap> {
        self.pending.clone()
    }
    pub fn notify_routes(&self) -> Arc<DashMap<String, mpsc::Sender<serde_json::Value>>> {
        self.notify_routes.clone()
    }
    pub fn writer(&self) -> Arc<Mutex<W>> {
        self.writer.clone()
    }

    pub fn register_route(&self, method: &str, tx: mpsc::Sender<serde_json::Value>) {
        self.notify_routes.insert(method.to_string(), tx);
    }

    /// Send a request and await its response (timeout applies).
    pub async fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.request_with_timeout(method, params, DEFAULT_REQUEST_TIMEOUT)
            .await
    }

    pub async fn request_with_timeout(
        &self,
        method: &str,
        params: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        let frame = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        {
            let mut w = self.writer.lock().await;
            core_engine::framing::write_ndjson(&mut *w, &frame).await?;
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(v))) => Ok(v),
            Ok(Ok(Err(e))) => Err(anyhow::Error::from(e)),
            Ok(Err(_)) => Err(anyhow!("response channel dropped")),
            Err(_) => {
                self.pending.inner.remove(&id);
                Err(anyhow!("request `{method}` timed out"))
            }
        }
    }

    pub async fn notify(&self, method: &str, params: serde_json::Value) -> Result<()> {
        let frame = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let mut w = self.writer.lock().await;
        core_engine::framing::write_ndjson(&mut *w, &frame).await?;
        Ok(())
    }
}

/// Spawn a background task that reads NDJSON frames from `reader` and
/// dispatches them to either the pending-request map (responses) or the
/// notification routes (notifications).
pub fn spawn_reader<R>(
    reader: R,
    pending: Arc<PendingMap>,
    notify_routes: Arc<DashMap<String, mpsc::Sender<serde_json::Value>>>,
) -> tokio::task::JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let (raw_tx, mut raw_rx) = mpsc::channel::<serde_json::Value>(64);
    core_engine::framing::spawn_ndjson_reader(
        reader,
        core_engine::framing::DEFAULT_MAX_LINE,
        raw_tx,
    );
    tokio::spawn(async move {
        while let Some(value) = raw_rx.recv().await {
            let frame: Frame = match serde_json::from_value(value.clone()) {
                Ok(f) => f,
                Err(e) => {
                    warn!(error = %e, "acp frame parse failed");
                    continue;
                }
            };
            match (frame.id, frame.method.clone(), frame.error, frame.result) {
                (Some(id), None, None, Some(result)) => pending.resolve(id, Ok(result)),
                (Some(id), None, Some(err), _) => pending.resolve(id, Err(err)),
                (_, Some(method), _, _) => {
                    let params = frame.params.unwrap_or(serde_json::Value::Null);
                    if let Some(route) = notify_routes.get(&method) {
                        if route.send(params).await.is_err() {
                            debug!(%method, "notification route dropped");
                        }
                    } else {
                        debug!(%method, "no route for notification");
                    }
                }
                _ => warn!("unknown jsonrpc frame shape"),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_response_round_trip() {
        // Use tokio duplex to wire client writer ↔ server reader, and the
        // reverse for responses.
        let (cw, mut sr) = tokio::io::duplex(4096);
        let (mut sw, cr) = tokio::io::duplex(4096);
        let client = JsonRpcClient::new(cw);
        let pending = client.pending();
        let routes = client.notify_routes();
        spawn_reader(cr, pending, routes);

        // Server task: read the request, echo back as result.
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt;
            let mut br = tokio::io::BufReader::new(&mut sr).lines();
            let line = br.next_line().await.unwrap().unwrap();
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            let resp = serde_json::json!({
                "jsonrpc": "2.0",
                "id": v["id"],
                "result": {"echo": v["params"]}
            });
            core_engine::framing::write_ndjson(&mut sw, &resp)
                .await
                .unwrap();
        });

        let res = client
            .request("ping", serde_json::json!({"x":1}))
            .await
            .unwrap();
        assert_eq!(res["echo"]["x"], serde_json::json!(1));
    }

    #[tokio::test]
    async fn notification_routed_to_method_channel() {
        let (cw, _sr) = tokio::io::duplex(1024);
        let (mut sw, cr) = tokio::io::duplex(1024);
        let client = JsonRpcClient::new(cw);
        let (tx, mut rx) = mpsc::channel(4);
        client.register_route("session/update", tx);
        spawn_reader(cr, client.pending(), client.notify_routes());

        let frame = serde_json::json!({
            "jsonrpc":"2.0",
            "method":"session/update",
            "params":{"sessionId":"s1","update":"hi"}
        });
        core_engine::framing::write_ndjson(&mut sw, &frame)
            .await
            .unwrap();
        let got = rx.recv().await.unwrap();
        assert_eq!(got["sessionId"], "s1");
    }
}
