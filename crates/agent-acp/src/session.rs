//! Live ACP subprocess wrapped as an [`AgentSession`].
//!
//! Lifecycle:
//! 1. Spawn binary, wire stdin/stdout to a [`JsonRpcClient`].
//! 2. `initialize` request → `client/initialized` notification.
//! 3. `session/new` (or `session/load` if resuming) → store the returned
//!    session id.
//! 4. Subscribe to `session/update` and `session/request_permission`
//!    notifications, translating into [`Event`]s.
//! 5. `send` writes a `session/prompt` request; the agent streams updates
//!    back via the notification channel.
//! 6. `close` sends `session/cancel`, then graceful shutdown.

use std::process::Stdio;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use core_traits::{
    AgentSession, Attachment, AttachmentKind, Event, PermissionRequest, Result, SessionKey,
};
use dashmap::DashMap;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, info, warn};

use crate::jsonrpc::{spawn_reader, JsonRpcClient};
use crate::protocol::{
    self, ClientCapabilities, ClientInfo, FsCapabilities, ImageSource, InitializeParams,
    InitializeResult, PermissionRequestParams, PromptBlock, PromptParams, SessionNewParams,
    SessionNewResult, SessionUpdate, UpdateBody,
};
use crate::AcpConfig;

use core_engine::framing::{EVENTS_CAP, SHUTDOWN_GRACE};

pub struct AcpSession {
    session_id: Arc<RwLock<String>>,
    client: Arc<JsonRpcClient<BufWriter<ChildStdin>>>,
    events_rx: Option<mpsc::Receiver<Event>>,
    pending_perms: Arc<DashMap<String, oneshot::Sender<bool>>>,
    child: Option<Child>,
}

impl AcpSession {
    pub async fn spawn(
        cfg: Arc<AcpConfig>,
        _key: SessionKey,
        resume: Option<String>,
    ) -> Result<Self> {
        let mut cmd = Command::new(&cfg.binary);
        for a in &cfg.args {
            cmd.arg(a);
        }
        if let Some(cwd) = &cfg.cwd {
            cmd.current_dir(cwd);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        debug!(binary = %cfg.binary, "spawning ACP agent");
        let mut child = cmd.spawn().context("spawn acp agent")?;
        let stdin = child.stdin.take().ok_or_else(|| anyhow!("no stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;

        let client = Arc::new(JsonRpcClient::new(BufWriter::new(stdin)));
        let pending = client.pending();
        let routes = client.notify_routes();
        spawn_reader(stdout, pending, routes);

        // ---------- handshake ----------
        let init_res: InitializeResult = serde_json::from_value(
            client
                .request(
                    "initialize",
                    serde_json::to_value(InitializeParams {
                        protocol_version: protocol::PROTOCOL_VERSION,
                        client_capabilities: ClientCapabilities {
                            fs: FsCapabilities {
                                read_text_file: true,
                                write_text_file: true,
                            },
                            client_info: ClientInfo {
                                name: "ai-agent-bridge",
                                version: env!("CARGO_PKG_VERSION"),
                            },
                        },
                    })?,
                )
                .await?,
        )?;
        info!(protocol = init_res.protocol_version, "ACP initialized");
        client
            .notify(
                "client/initialized",
                serde_json::Value::Object(Default::default()),
            )
            .await?;

        // ---------- session/new or session/load ----------
        let cwd = cfg
            .cwd
            .as_deref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| {
                std::env::current_dir()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default()
            });

        let session_id = if let Some(prev) = resume {
            let res = client
                .request(
                    "session/load",
                    serde_json::json!({"sessionId": prev, "cwd": cwd}),
                )
                .await;
            match res {
                Ok(_) => prev,
                Err(e) => {
                    warn!(error = %e, "session/load failed; falling back to session/new");
                    let r: SessionNewResult = serde_json::from_value(
                        client
                            .request(
                                "session/new",
                                serde_json::to_value(SessionNewParams {
                                    cwd: &cwd,
                                    mcp_servers: vec![],
                                })?,
                            )
                            .await?,
                    )?;
                    r.session_id
                }
            }
        } else {
            let r: SessionNewResult = serde_json::from_value(
                client
                    .request(
                        "session/new",
                        serde_json::to_value(SessionNewParams {
                            cwd: &cwd,
                            mcp_servers: vec![],
                        })?,
                    )
                    .await?,
            )?;
            r.session_id
        };

        // ---------- subscribe to streaming notifications ----------
        let (update_tx, mut update_rx) = mpsc::channel::<serde_json::Value>(EVENTS_CAP);
        let (perm_tx, mut perm_rx) = mpsc::channel::<serde_json::Value>(EVENTS_CAP);
        client.register_route("session/update", update_tx);
        client.register_route("session/request_permission", perm_tx);

        let (events_tx, events_rx) = mpsc::channel(EVENTS_CAP);
        let pending_perms: Arc<DashMap<String, oneshot::Sender<bool>>> = Arc::new(DashMap::new());
        let session_id_arc = Arc::new(RwLock::new(session_id.clone()));
        {
            let sid = session_id_arc.clone();
            let etx = events_tx.clone();
            tokio::spawn(async move {
                while let Some(value) = update_rx.recv().await {
                    let upd: SessionUpdate = match serde_json::from_value(value) {
                        Ok(u) => u,
                        Err(e) => {
                            warn!(error = %e, "session/update parse failed");
                            continue;
                        }
                    };
                    if upd.session_id != *sid.read().await {
                        continue;
                    }
                    let mapped = match upd.update {
                        UpdateBody::AgentMessageChunk { content } => match content {
                            protocol::ContentBlock::Text { text } if !text.is_empty() => {
                                Some(Event::AssistantText {
                                    text,
                                    partial: true,
                                })
                            }
                            _ => None,
                        },
                        UpdateBody::ToolCall {
                            tool_call_id,
                            title,
                            ..
                        } => Some(Event::ToolStart {
                            id: tool_call_id,
                            name: title.unwrap_or_default(),
                        }),
                        UpdateBody::ToolCallUpdate {
                            tool_call_id,
                            status,
                        } => {
                            let ok = status.as_deref() == Some("completed");
                            if status.as_deref() == Some("in_progress") {
                                None
                            } else {
                                Some(Event::ToolEnd {
                                    id: tool_call_id,
                                    ok,
                                })
                            }
                        }
                        UpdateBody::Plan { .. } | UpdateBody::Other => None,
                    };
                    if let Some(evt) = mapped {
                        if etx.send(evt).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
        {
            let sid = session_id_arc.clone();
            let etx = events_tx.clone();
            let perms = pending_perms.clone();
            tokio::spawn(async move {
                while let Some(value) = perm_rx.recv().await {
                    let p: PermissionRequestParams = match serde_json::from_value(value) {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(error = %e, "permission request parse failed");
                            continue;
                        }
                    };
                    if p.session_id != *sid.read().await {
                        continue;
                    }
                    let (ptx, _prx) = oneshot::channel::<bool>();
                    perms.insert(p.request_id.clone(), ptx);
                    let tool_name = p
                        .tool_call
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let description = p
                        .tool_call
                        .get("kind")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let evt = Event::PermissionRequest(PermissionRequest {
                        id: p.request_id,
                        tool_name,
                        description,
                        input: p.tool_call,
                    });
                    if etx.send(evt).await.is_err() {
                        break;
                    }
                }
            });
        }

        Ok(Self {
            session_id: session_id_arc,
            client,
            events_rx: Some(events_rx),
            pending_perms,
            child: Some(child),
        })
    }
}

#[async_trait]
impl AgentSession for AcpSession {
    fn id(&self) -> String {
        // Best-effort sync read: session id is set at spawn and rarely changes.
        self.session_id
            .try_read()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    async fn send(&mut self, prompt: String, attachments: Vec<Attachment>) -> Result<()> {
        let mut blocks: Vec<PromptBlock> = Vec::new();
        if !prompt.is_empty() {
            blocks.push(PromptBlock::Text { text: prompt });
        }
        for att in attachments {
            if matches!(att.kind, AttachmentKind::Image) {
                blocks.push(PromptBlock::Image {
                    source: ImageSource::Path {
                        path: att.path.to_string_lossy().to_string(),
                        media_type: att.mime,
                    },
                });
            } else {
                blocks.push(PromptBlock::Text {
                    text: format!("[attachment: {}]", att.path.display()),
                });
            }
        }
        let session_id = self.session_id.read().await.clone();
        self.client
            .request(
                "session/prompt",
                serde_json::to_value(PromptParams {
                    session_id: &session_id,
                    prompt: blocks,
                })?,
            )
            .await
            .map(|_| ())
    }

    fn events(&mut self) -> mpsc::Receiver<Event> {
        self.events_rx.take().expect("events() called twice")
    }

    async fn answer_permission(&mut self, id: String, allow: bool) -> Result<()> {
        if let Some((_, tx)) = self.pending_perms.remove(&id) {
            let _ = tx.send(allow);
        }
        let session_id = self.session_id.read().await.clone();
        // Spec uses `session/respond_to_permission_request`; outcome shape is
        // {selected: { id: "allow" | "deny" }} in newer drafts.
        let outcome = if allow { "allowed" } else { "denied" };
        self.client
            .notify(
                "session/respond_to_permission_request",
                serde_json::json!({
                    "sessionId": session_id,
                    "requestId": id,
                    "outcome": {"type": outcome},
                }),
            )
            .await
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        let session_id = self.session_id.read().await.clone();
        let _ = self
            .client
            .notify(
                "session/cancel",
                serde_json::json!({"sessionId": session_id}),
            )
            .await;
        if let Ok(mut w) = self.client.writer().try_lock() {
            let _ = w.shutdown().await;
        }
        if let Some(mut child) = self.child.take() {
            core_engine::framing::shutdown_child(&mut child, SHUTDOWN_GRACE).await;
        }
        Ok(())
    }
}
