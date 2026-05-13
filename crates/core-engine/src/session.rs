//! One actor task per live session. The actor owns the [`AgentSession`] and
//! serializes both inbound commands (Send/Reset/Permission/Close) and outbound
//! events (assistant text, attachments, permission requests) → platform replies.
//!
//! Streaming throttle: partial chunks (`Event::AssistantText { partial: true }`)
//! are buffered and flushed every [`FLUSH_INTERVAL`] OR every
//! [`FLUSH_THRESHOLD_BYTES`], whichever comes first. A non-partial chunk or
//! [`Event::Done`] always force-flushes. This keeps chat platforms (LINE rate
//! limits, Slack flood detection) sane while still feeling streaming-ish.
//!
//! When `batch_replies` is true, partial text is never flushed mid-stream.
//! The actor waits for the complete non-partial text and sends it as a single
//! message. This avoids fragmented replies on chat platforms.

use std::sync::Arc;
use std::time::Duration;

use core_traits::{AgentSession, Attachment, Event, Platform, ReplyCtx, SessionKey};
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::registry::SessionRegistry;
use crate::scheduler::Scheduler;

const INBOX_CAP: usize = 32;
const FLUSH_INTERVAL: Duration = Duration::from_millis(1200);
const FLUSH_THRESHOLD_BYTES: usize = 240;
const THINKING_INTERVAL: Duration = Duration::from_secs(3);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(1800);

#[derive(Debug)]
pub enum Cmd {
    Send {
        prompt: String,
        attachments: Vec<Attachment>,
        reply_ctx: ReplyCtx,
    },
    Permission {
        id: String,
        allow: bool,
    },
    Close,
}

#[derive(Clone)]
pub struct SessionHandle {
    pub id: String,
    pub key: SessionKey,
    pub tx: mpsc::Sender<Cmd>,
}

pub struct SessionActor {
    pub tx: mpsc::Sender<Cmd>,
}

impl SessionActor {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        session: Box<dyn AgentSession>,
        platform: Arc<dyn Platform>,
        reply_ctx: ReplyCtx,
        registry: Arc<Mutex<SessionRegistry>>,
        key: SessionKey,
        agent_name: String,
        batch_replies: bool,
        scheduler: Option<Arc<Scheduler>>,
    ) -> Self {
        Self::spawn_with_idle_timeout(
            session,
            platform,
            reply_ctx,
            registry,
            key,
            agent_name,
            batch_replies,
            DEFAULT_IDLE_TIMEOUT,
            scheduler,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_with_idle_timeout(
        mut session: Box<dyn AgentSession>,
        platform: Arc<dyn Platform>,
        reply_ctx: ReplyCtx,
        registry: Arc<Mutex<SessionRegistry>>,
        key: SessionKey,
        agent_name: String,
        batch_replies: bool,
        idle_timeout: Duration,
        scheduler: Option<Arc<Scheduler>>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Cmd>(INBOX_CAP);
        let mut events = session.events();
        let session_id = session.id();
        tokio::spawn(async move {
            info!(?key, agent = %agent_name, sid = %session_id, batch = batch_replies, "session actor up");
            let mut state = StreamState::new(batch_replies);
            let mut reply_ctx = reply_ctx;
            let mut last_activity = Instant::now();
            loop {
                let deadline = state.next_deadline();
                let idle_deadline = last_activity + idle_timeout;
                let effective_deadline = match deadline {
                    Some(d) => Some(d.min(idle_deadline)),
                    None => Some(idle_deadline),
                };
                tokio::select! {
                    cmd = rx.recv() => match cmd {
                        Some(Cmd::Send { prompt, attachments, reply_ctx: ctx }) => {
                            last_activity = Instant::now();
                            reply_ctx = ctx;
                            state.discard_buffer();
                            if let Err(e) = session.send(prompt, attachments).await {
                                error!(error = %e, "session.send failed");
                                let _ = platform.reply(&reply_ctx, &format!("agent error: {e}")).await;
                            } else {
                                state.waiting_for_response = true;
                                state.last_thinking = Some(Instant::now());
                            }
                        }
                        Some(Cmd::Permission { id, allow }) => {
                            last_activity = Instant::now();
                            if let Err(e) = session.answer_permission(id, allow).await {
                                warn!(error = %e, "answer_permission failed");
                            }
                        }
                        Some(Cmd::Close) | None => {
                            state.flush(&platform, &reply_ctx).await;
                            break;
                        }
                    },
                    evt = events.recv() => match evt {
                        Some(e) => {
                            last_activity = Instant::now();
                            state.waiting_for_response = false;
                            handle_event(
                                &platform, &reply_ctx, e, &registry, &key, &mut state,
                                &scheduler,
                            ).await;
                        }
                        None => {
                            debug!("agent events stream ended; closing session");
                            state.flush(&platform, &reply_ctx).await;
                            break;
                        }
                    },
                    _ = sleep_until(effective_deadline) => {
                        if last_activity.elapsed() >= idle_timeout && !state.processing && !state.waiting_for_response {
                            info!(?key, "session idle timeout; closing");
                            state.flush(&platform, &reply_ctx).await;
                            break;
                        }
                        if (state.batch && state.processing) || state.waiting_for_response {
                            state.send_thinking(&platform, &reply_ctx).await;
                        } else {
                            state.flush(&platform, &reply_ctx).await;
                        }
                    }
                }
            }
            if let Err(e) = session.close().await {
                warn!(error = %e, "session close failed");
            }
            info!(?key, "session actor down");
        });
        Self { tx }
    }
}

/// Sleeps until `deadline`. If `deadline` is `None`, returns a future that
/// never completes (so the outer `select!` only fires on cmd or events).
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

#[derive(Default)]
struct StreamState {
    buf: String,
    /// When buf became non-empty; flush at this + FLUSH_INTERVAL.
    buffering_since: Option<Instant>,
    /// When true, never flush partials; wait for non-partial or Done.
    batch: bool,
    /// True while receiving events but haven't sent the final reply yet.
    processing: bool,
    /// Last time we sent a thinking indicator.
    last_thinking: Option<Instant>,
    /// Counter for cycling through thinking messages.
    thinking_count: usize,
    /// True between session.send() and the first event from the agent.
    /// Prevents idle timeout from killing sessions where the agent is
    /// thinking but hasn't emitted its first token yet.
    waiting_for_response: bool,
}

impl StreamState {
    fn new(batch: bool) -> Self {
        Self {
            batch,
            ..Default::default()
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        if self.batch {
            if self.processing || self.waiting_for_response {
                let base = self.last_thinking.unwrap_or_else(Instant::now);
                return Some(base + THINKING_INTERVAL);
            }
            return None;
        }
        if self.waiting_for_response {
            let base = self.last_thinking.unwrap_or_else(Instant::now);
            return Some(base + THINKING_INTERVAL);
        }
        self.buffering_since.map(|t| t + FLUSH_INTERVAL)
    }

    fn mark_processing(&mut self) {
        if !self.processing {
            self.processing = true;
            self.last_thinking = Some(Instant::now());
        }
    }

    fn clear_processing(&mut self) {
        self.processing = false;
        self.last_thinking = None;
        self.thinking_count = 0;
    }

    async fn send_thinking(&mut self, platform: &Arc<dyn Platform>, reply_ctx: &ReplyCtx) {
        self.thinking_count += 1;
        self.last_thinking = Some(Instant::now());
        let _ = platform.show_typing(reply_ctx).await;
    }

    fn append_partial(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        if self.buf.is_empty() {
            self.buffering_since = Some(Instant::now());
        }
        self.buf.push_str(text);
    }

    fn should_flush(&self) -> bool {
        !self.batch && self.buf.len() >= FLUSH_THRESHOLD_BYTES
    }

    fn discard_buffer(&mut self) {
        self.buf.clear();
        self.buffering_since = None;
    }

    async fn flush(&mut self, platform: &Arc<dyn Platform>, reply_ctx: &ReplyCtx) {
        self.clear_processing();
        if self.buf.trim().is_empty() {
            self.discard_buffer();
            return;
        }
        let text = std::mem::take(&mut self.buf);
        self.buffering_since = None;
        if let Err(e) = platform.reply(reply_ctx, &text).await {
            warn!(error = %e, "platform.reply failed during flush");
        }
    }
}

const AAB_TAG_START: &str = "<!--aab:";
const AAB_TAG_END: &str = "-->";

async fn process_aab_commands(
    text: &str,
    scheduler: &Option<Arc<Scheduler>>,
    key: &SessionKey,
    reply_ctx: &ReplyCtx,
) -> (String, Vec<String>) {
    let mut cleaned = text.to_string();
    let mut extra_msgs: Vec<String> = Vec::new();

    let Some(sched) = scheduler else {
        return (cleaned, extra_msgs);
    };

    while let Some(start) = cleaned.find(AAB_TAG_START) {
        let Some(end) = cleaned[start..].find(AAB_TAG_END) else {
            break;
        };
        let end_abs = start + end + AAB_TAG_END.len();
        let inner = &cleaned[start + AAB_TAG_START.len()..start + end]
            .trim()
            .to_string();
        let tag_full = cleaned[start..end_abs].to_string();

        let (cmd, body) = match inner.find(|c: char| c.is_whitespace()) {
            Some(pos) => (inner[..pos].to_string(), inner[pos..].trim().to_string()),
            None => (inner.clone(), String::new()),
        };

        match cmd.as_str() {
            "schedule" => match serde_json::from_str::<AabScheduleReq>(&body) {
                Ok(req) => match crate::scheduler::parse_schedule(&req.when) {
                    Ok(schedule) => {
                        let entry = crate::scheduler::ScheduleEntry {
                            id: uuid::Uuid::new_v4().to_string(),
                            key: key.clone(),
                            prompt: req.prompt.clone(),
                            reply_ctx: reply_ctx.clone(),
                            schedule,
                            created_at_ms: core_traits::now_ms(),
                            last_fired_ms: None,
                        };
                        let display = entry.display_schedule();
                        let id_short = entry.id[..8.min(entry.id.len())].to_string();
                        match sched.add(entry).await {
                            Ok(()) => {
                                debug!(id = %id_short, "aab:schedule created");
                                extra_msgs.push(format!(
                                    "✅ scheduled (id={id_short}):\n  {display}\n  prompt: \"{}\"",
                                    req.prompt
                                ));
                            }
                            Err(e) => {
                                warn!(error = %e, "aab:schedule add failed");
                                extra_msgs.push(format!("⚠️ schedule failed: {e}"));
                            }
                        }
                    }
                    Err(e) => {
                        extra_msgs.push(format!("⚠️ invalid schedule time: {e}"));
                    }
                },
                Err(e) => {
                    warn!(error = %e, body = %body, "aab:schedule parse failed");
                }
            },
            "schedule-list" => {
                let entries = sched.list(key).await;
                if entries.is_empty() {
                    extra_msgs.push("📋 no scheduled actions.".into());
                } else {
                    let mut lines = vec![format!("📋 Scheduled actions ({}):", entries.len())];
                    for e in &entries {
                        lines.push(format!(
                            "  [{}] {} — \"{}\"",
                            &e.id[..8.min(e.id.len())],
                            e.display_schedule(),
                            e.prompt,
                        ));
                    }
                    extra_msgs.push(lines.join("\n"));
                }
            }
            "schedule-delete" => match serde_json::from_str::<AabScheduleDeleteReq>(&body) {
                Ok(req) => {
                    let entries = sched.list(key).await;
                    let matches: Vec<_> = entries
                        .iter()
                        .filter(|e| e.id.starts_with(&req.id))
                        .collect();
                    match matches.len() {
                        0 => extra_msgs.push(format!("⚠️ no schedule matching `{}`", req.id)),
                        1 => {
                            let full_id = matches[0].id.clone();
                            match sched.remove(key, &full_id).await {
                                Ok(true) => {
                                    extra_msgs.push(format!(
                                        "🗑️ schedule `{}` deleted",
                                        &full_id[..8.min(full_id.len())]
                                    ));
                                }
                                _ => extra_msgs.push("⚠️ failed to delete schedule".into()),
                            }
                        }
                        n => extra_msgs.push(format!(
                            "⚠️ `{}` matches {n} schedules — use a longer prefix",
                            req.id
                        )),
                    }
                }
                Err(e) => {
                    warn!(error = %e, body = %body, "aab:schedule-delete parse failed");
                }
            },
            other => {
                debug!(cmd = other, "unknown aab command; ignoring");
            }
        }

        cleaned = cleaned.replace(&tag_full, "");
    }

    let cleaned = cleaned.trim().to_string();
    (cleaned, extra_msgs)
}

#[derive(serde::Deserialize)]
struct AabScheduleReq {
    when: String,
    prompt: String,
}

#[derive(serde::Deserialize)]
struct AabScheduleDeleteReq {
    id: String,
}

async fn handle_event(
    platform: &Arc<dyn Platform>,
    reply_ctx: &ReplyCtx,
    event: Event,
    registry: &Arc<Mutex<SessionRegistry>>,
    key: &SessionKey,
    state: &mut StreamState,
    scheduler: &Option<Arc<Scheduler>>,
) {
    match event {
        Event::AssistantText { text, partial } => {
            state.mark_processing();
            if partial {
                state.append_partial(&text);
                if state.should_flush() {
                    state.flush(platform, reply_ctx).await;
                }
            } else {
                let (cleaned, extra_msgs) =
                    process_aab_commands(&text, scheduler, key, reply_ctx).await;

                state.clear_processing();
                if !state.buf.is_empty() && text.starts_with(&state.buf) {
                    state.discard_buffer();
                    if !cleaned.trim().is_empty() {
                        let _ = platform.reply(reply_ctx, &cleaned).await;
                    }
                } else if !state.buf.is_empty() {
                    state.flush(platform, reply_ctx).await;
                    if !cleaned.trim().is_empty() {
                        let _ = platform.reply(reply_ctx, &cleaned).await;
                    }
                } else if !cleaned.trim().is_empty() {
                    let _ = platform.reply(reply_ctx, &cleaned).await;
                }
                for msg in extra_msgs {
                    let _ = platform.reply(reply_ctx, &msg).await;
                }
            }
        }
        Event::AssistantAttachment(att) => {
            state.flush(platform, reply_ctx).await;
            if let Err(e) = platform.send_attachment(reply_ctx, &att).await {
                warn!(error = %e, path = %att.path.display(), "send_attachment failed");
                let msg = format!(
                    "file upload failed for `{}`: {e}",
                    att.name.as_deref().unwrap_or("file")
                );
                let _ = platform.reply(reply_ctx, &msg).await;
            }
        }
        Event::PermissionRequest(req) => {
            state.flush(platform, reply_ctx).await;
            let body = format!(
                "permission for `{}` (id={})\n{}\nReply `/yes {}` or `/no {}` to decide.",
                req.tool_name, req.id, req.description, req.id, req.id
            );
            let _ = platform.reply(reply_ctx, &body).await;
        }
        Event::ToolStart { name, .. } => {
            state.mark_processing();
            debug!("tool start: {name}");
        }
        Event::ToolEnd { id, ok } => {
            debug!("tool end: {id} ok={ok}");
        }
        Event::Error(msg) => {
            state.flush(platform, reply_ctx).await;
            let _ = platform.reply(reply_ctx, &format!("⚠️ {msg}")).await;
            let mut reg = registry.lock().await;
            reg.clear_active(key);
        }
        Event::Done { session_id } => {
            state.flush(platform, reply_ctx).await;
            let mut reg = registry.lock().await;
            if let Some(entry) = reg.get_session(key) {
                reg.record_session(key.clone(), entry.agent, session_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── StreamState unit tests ────────────────────────────────────────────

    #[test]
    fn append_partial_sets_buffering_since() {
        let mut s = StreamState::new(false);
        assert!(s.buffering_since.is_none());
        s.append_partial("hello");
        assert!(s.buffering_since.is_some());
        assert_eq!(s.buf, "hello");
    }

    #[test]
    fn append_partial_empty_text_is_noop() {
        let mut s = StreamState::new(false);
        s.append_partial("");
        assert!(s.buffering_since.is_none());
        assert!(s.buf.is_empty());
    }

    #[test]
    fn should_flush_false_when_under_threshold() {
        let mut s = StreamState::new(false);
        s.append_partial("short");
        assert!(!s.should_flush());
    }

    #[test]
    fn should_flush_true_when_over_threshold() {
        let mut s = StreamState::new(false);
        s.append_partial(&"x".repeat(FLUSH_THRESHOLD_BYTES + 1));
        assert!(s.should_flush());
    }

    #[test]
    fn should_flush_false_in_batch_mode() {
        let mut s = StreamState::new(true);
        s.append_partial(&"x".repeat(FLUSH_THRESHOLD_BYTES + 100));
        assert!(!s.should_flush());
    }

    #[test]
    fn discard_buffer_clears_all() {
        let mut s = StreamState::new(false);
        s.append_partial("data");
        assert!(!s.buf.is_empty());
        assert!(s.buffering_since.is_some());
        s.discard_buffer();
        assert!(s.buf.is_empty());
        assert!(s.buffering_since.is_none());
    }

    #[tokio::test]
    async fn flush_sends_text_and_clears_buffer() {
        let platform = Arc::new(test_support::MockPlatform::new("t"));
        let ctx = ReplyCtx::default();
        let mut s = StreamState::new(false);
        s.append_partial("hello world");
        s.flush(&(platform.clone() as Arc<dyn Platform>), &ctx)
            .await;
        assert!(s.buf.is_empty());
        assert!(s.buffering_since.is_none());
        let replies = platform.replies().await;
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].text, "hello world");
    }

    #[tokio::test]
    async fn flush_skips_whitespace_only() {
        let platform = Arc::new(test_support::MockPlatform::new("t"));
        let ctx = ReplyCtx::default();
        let mut s = StreamState::new(false);
        s.append_partial("   \n  ");
        s.flush(&(platform.clone() as Arc<dyn Platform>), &ctx)
            .await;
        let replies = platform.replies().await;
        assert!(replies.is_empty());
        assert!(s.buf.is_empty());
    }

    #[test]
    fn mark_processing_idempotent() {
        let mut s = StreamState::new(true);
        assert!(!s.processing);
        s.mark_processing();
        assert!(s.processing);
        let first_ts = s.last_thinking;
        s.mark_processing();
        assert_eq!(s.last_thinking, first_ts);
    }

    #[test]
    fn clear_processing_resets_state() {
        let mut s = StreamState::new(true);
        s.mark_processing();
        s.thinking_count = 5;
        s.clear_processing();
        assert!(!s.processing);
        assert!(s.last_thinking.is_none());
        assert_eq!(s.thinking_count, 0);
    }

    #[test]
    fn next_deadline_none_when_idle_streaming() {
        let s = StreamState::new(false);
        assert!(s.next_deadline().is_none());
    }

    #[test]
    fn next_deadline_none_when_idle_batch() {
        let s = StreamState::new(true);
        assert!(s.next_deadline().is_none());
    }

    #[test]
    fn next_deadline_returns_thinking_interval_in_batch_processing() {
        let mut s = StreamState::new(true);
        s.mark_processing();
        let deadline = s.next_deadline();
        assert!(deadline.is_some());
    }

    #[test]
    fn next_deadline_returns_flush_interval_when_buffered_streaming() {
        let mut s = StreamState::new(false);
        s.append_partial("data");
        let deadline = s.next_deadline();
        assert!(deadline.is_some());
    }

    #[test]
    fn waiting_for_response_prevents_idle_deadline_from_being_none() {
        let mut s = StreamState::new(false);
        assert!(s.next_deadline().is_none());
        s.waiting_for_response = true;
        s.last_thinking = Some(Instant::now());
        assert!(s.next_deadline().is_some());
    }

    #[test]
    fn waiting_for_response_batch_mode_returns_deadline() {
        let mut s = StreamState::new(true);
        assert!(s.next_deadline().is_none());
        s.waiting_for_response = true;
        s.last_thinking = Some(Instant::now());
        assert!(s.next_deadline().is_some());
    }

    #[test]
    fn waiting_for_response_default_false() {
        let s = StreamState::new(false);
        assert!(!s.waiting_for_response);
    }

    // ── process_aab_commands tests ────────────────────────────────────────

    fn make_test_scheduler() -> Arc<Scheduler> {
        use crate::{Engine, StateDb};
        let platform = Arc::new(test_support::MockPlatform::new("t"));
        let db = Arc::new(Mutex::new(StateDb::in_memory()));
        let engine = Engine::builder()
            .add_agent(Arc::new(test_support::EchoAgent))
            .default_agent("echo")
            .platform(platform)
            .build()
            .unwrap();
        Scheduler::spawn(engine, db)
    }

    #[tokio::test]
    async fn aab_schedule_creates_entry() {
        let sched = make_test_scheduler();
        let key = SessionKey::new("t", "u1");
        let ctx = ReplyCtx::default();
        let text = r#"OK!<!--aab:schedule {"when":"in 30m","prompt":"hello"}-->"#;
        let (cleaned, msgs) = process_aab_commands(text, &Some(sched.clone()), &key, &ctx).await;
        assert_eq!(cleaned, "OK!");
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].contains("scheduled"));
        let entries = sched.list(&key).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].prompt, "hello");
    }

    #[tokio::test]
    async fn aab_schedule_list_empty() {
        let sched = make_test_scheduler();
        let key = SessionKey::new("t", "u2");
        let ctx = ReplyCtx::default();
        let text = "Here:<!--aab:schedule-list-->";
        let (cleaned, msgs) = process_aab_commands(text, &Some(sched), &key, &ctx).await;
        assert_eq!(cleaned, "Here:");
        assert!(msgs[0].contains("no scheduled"));
    }

    #[tokio::test]
    async fn aab_schedule_list_shows_entries() {
        let sched = make_test_scheduler();
        let key = SessionKey::new("t", "u3");
        let ctx = ReplyCtx::default();
        // Create an entry first
        let entry = crate::scheduler::ScheduleEntry {
            id: "test-id-1234".into(),
            key: key.clone(),
            prompt: "remind me".into(),
            reply_ctx: ctx.clone(),
            schedule: crate::scheduler::parse_schedule("in 1h").unwrap(),
            created_at_ms: 0,
            last_fired_ms: None,
        };
        sched.add(entry).await.unwrap();

        let text = "List:<!--aab:schedule-list-->";
        let (_, msgs) = process_aab_commands(text, &Some(sched), &key, &ctx).await;
        assert!(msgs[0].contains("remind me"));
        assert!(msgs[0].contains("test-id-"));
    }

    #[tokio::test]
    async fn aab_schedule_delete_removes() {
        let sched = make_test_scheduler();
        let key = SessionKey::new("t", "u4");
        let ctx = ReplyCtx::default();
        let entry = crate::scheduler::ScheduleEntry {
            id: "deadbeef-1234".into(),
            key: key.clone(),
            prompt: "test".into(),
            reply_ctx: ctx.clone(),
            schedule: crate::scheduler::parse_schedule("in 1h").unwrap(),
            created_at_ms: 0,
            last_fired_ms: None,
        };
        sched.add(entry).await.unwrap();

        let text = r#"Done<!--aab:schedule-delete {"id":"deadbeef"}-->"#;
        let (cleaned, msgs) = process_aab_commands(text, &Some(sched.clone()), &key, &ctx).await;
        assert_eq!(cleaned, "Done");
        assert!(msgs[0].contains("deleted"));
        assert!(sched.list(&key).await.is_empty());
    }

    #[tokio::test]
    async fn aab_no_scheduler_passes_through() {
        let key = SessionKey::new("t", "u5");
        let ctx = ReplyCtx::default();
        let text = r#"hi<!--aab:schedule {"when":"in 1h","prompt":"x"}-->"#;
        let (cleaned, msgs) = process_aab_commands(text, &None, &key, &ctx).await;
        assert_eq!(cleaned, text);
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn aab_multiple_commands() {
        let sched = make_test_scheduler();
        let key = SessionKey::new("t", "u6");
        let ctx = ReplyCtx::default();
        let text = r#"A<!--aab:schedule {"when":"in 1h","prompt":"one"}-->B<!--aab:schedule {"when":"in 2h","prompt":"two"}-->C"#;
        let (cleaned, msgs) = process_aab_commands(text, &Some(sched.clone()), &key, &ctx).await;
        assert_eq!(cleaned, "ABC");
        assert_eq!(msgs.len(), 2);
        assert_eq!(sched.list(&key).await.len(), 2);
    }
}
