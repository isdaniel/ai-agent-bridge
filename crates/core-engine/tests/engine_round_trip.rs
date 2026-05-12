//! End-to-end engine wiring: a chat message flows in, the EchoAgent answers,
//! the MockPlatform records the reply.

use std::sync::Arc;

use core_engine::{Engine, Scheduler, SessionRegistry};
use core_traits::{Message, MessageHandler, ReplyCtx, SessionKey};
use test_support::{EchoAgent, MockPlatform};
use tokio::sync::Mutex;

fn make_msg(key: SessionKey, text: &str) -> Message {
    Message {
        key,
        text: text.into(),
        attachments: vec![],
        reply_ctx: ReplyCtx {
            user: Some("u1".into()),
            ..Default::default()
        },
        timestamp_ms: 0,
    }
}

async fn drain_until<F: Fn(&[test_support::RecordedReply]) -> bool>(
    platform: &Arc<MockPlatform>,
    pred: F,
) {
    for _ in 0..50 {
        if pred(&platform.replies().await) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn echo_round_trip() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform.clone())
        .build()
        .unwrap();

    let h: Arc<dyn MessageHandler> = engine.clone();
    h.handle(make_msg(SessionKey::new("mock", "u1"), "hello"))
        .await;

    drain_until(&platform, |r| r.iter().any(|x| x.text == "echo: hello")).await;
    let replies = platform.replies().await;
    assert!(
        replies.iter().any(|r| r.text == "echo: hello"),
        "expected echo reply, got: {:?}",
        replies.iter().map(|r| &r.text).collect::<Vec<_>>()
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn new_clears_active_session() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform.clone())
        .build()
        .unwrap();

    let h: Arc<dyn MessageHandler> = engine.clone();
    h.handle(make_msg(SessionKey::new("mock", "u2"), "/new"))
        .await;
    drain_until(&platform, |r| {
        r.iter().any(|x| x.text.contains("session reset"))
    })
    .await;
    let replies = platform.replies().await;
    assert!(replies.iter().any(|r| r.text.contains("session reset")));
    engine.shutdown().await;
}

#[tokio::test]
async fn help_lists_builtins() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform.clone())
        .build()
        .unwrap();

    let h: Arc<dyn MessageHandler> = engine.clone();
    h.handle(make_msg(SessionKey::new("mock", "uh"), "/help"))
        .await;
    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("/help"))).await;
    let replies = platform.replies().await;
    let body = &replies
        .iter()
        .find(|r| r.text.contains("/help"))
        .unwrap()
        .text;
    assert!(body.contains("/new"));
    assert!(body.contains("/resume"));
    assert!(body.contains("/mcp"));
    assert!(body.contains("/skills"));
    engine.shutdown().await;
}

#[tokio::test]
async fn agent_switch_unknown_returns_error() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform.clone())
        .build()
        .unwrap();
    let key = SessionKey::new("mock", "ux");
    let res = engine.switch_agent(&key, "no-such").await;
    assert!(res.is_err());
}

#[tokio::test]
async fn unknown_slash_replies_help_hint() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform.clone())
        .build()
        .unwrap();
    let h: Arc<dyn MessageHandler> = engine.clone();
    h.handle(make_msg(SessionKey::new("mock", "uu"), "/zonk x"))
        .await;
    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("unknown"))).await;
    let replies = platform.replies().await;
    assert!(replies.iter().any(|r| r.text.contains("unknown command")));
    engine.shutdown().await;
}

#[tokio::test]
async fn clear_wipes_all_history() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform.clone())
        .build()
        .unwrap();
    let key = SessionKey::new("mock", "uc");
    let h: Arc<dyn MessageHandler> = engine.clone();

    // Send a message to establish a session, then clear it.
    h.handle(make_msg(key.clone(), "setup")).await;
    drain_until(&platform, |r| r.iter().any(|x| x.text == "echo: setup")).await;

    h.handle(make_msg(key.clone(), "/clear")).await;
    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("cleared"))).await;
    let replies = platform.replies().await;
    assert!(replies.iter().any(|r| r.text.contains("history wiped")));

    engine.shutdown().await;
}

#[tokio::test]
async fn max_sessions_rejects_overflow() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform.clone())
        .max_sessions(2)
        .build()
        .unwrap();
    let h: Arc<dyn MessageHandler> = engine.clone();

    // First two distinct keys spawn fine.
    h.handle(make_msg(SessionKey::new("mock", "u-a"), "hi from a"))
        .await;
    h.handle(make_msg(SessionKey::new("mock", "u-b"), "hi from b"))
        .await;
    drain_until(&platform, |r| {
        r.iter().filter(|x| x.text.starts_with("echo: ")).count() >= 2
    })
    .await;

    // Third distinct key must be rejected with the capacity message.
    h.handle(make_msg(SessionKey::new("mock", "u-c"), "hi from c"))
        .await;
    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("capacity"))).await;
    let replies = platform.replies().await;
    assert!(
        replies.iter().any(|r| r.text.contains("at capacity")),
        "expected capacity rejection, got: {:?}",
        replies.iter().map(|r| &r.text).collect::<Vec<_>>()
    );
    // And no echo reply for u-c.
    assert!(!replies.iter().any(|r| r.text == "echo: hi from c"));

    // Re-using an existing key still works (within cap).
    h.handle(make_msg(SessionKey::new("mock", "u-a"), "again"))
        .await;
    drain_until(&platform, |r| r.iter().any(|x| x.text == "echo: again")).await;
    let replies = platform.replies().await;
    assert!(replies.iter().any(|r| r.text == "echo: again"));

    engine.shutdown().await;
}

// ── Schedule integration tests ──────────────────────────────────────────

fn make_engine_with_scheduler(platform: Arc<MockPlatform>) -> (Arc<Engine>, Arc<Scheduler>) {
    let registry = Arc::new(Mutex::new(SessionRegistry::in_memory()));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform)
        .registry(registry.clone())
        .build()
        .unwrap();
    let sched = Scheduler::spawn(engine.clone(), registry);
    engine.set_scheduler(sched.clone());
    (engine, sched)
}

#[tokio::test]
async fn schedule_creates_and_lists() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let (engine, _sched) = make_engine_with_scheduler(platform.clone());
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(
        SessionKey::new("mock", "u-sched"),
        "/schedule in 1h \"test prompt\"",
    ))
    .await;
    drain_until(&platform, |r| {
        r.iter().any(|x| x.text.contains("scheduled"))
    })
    .await;
    let replies = platform.replies().await;
    assert!(
        replies.iter().any(|r| r.text.contains("scheduled (id=")),
        "expected schedule confirmation"
    );

    // Now list schedules
    h.handle(make_msg(
        SessionKey::new("mock", "u-sched"),
        "/schedule-list",
    ))
    .await;
    drain_until(&platform, |r| {
        r.iter().any(|x| x.text.contains("Scheduled actions"))
    })
    .await;
    let replies = platform.replies().await;
    assert!(replies.iter().any(|r| r.text.contains("test prompt")));

    engine.shutdown().await;
}

#[tokio::test]
async fn schedule_delete_removes_entry() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let (engine, sched) = make_engine_with_scheduler(platform.clone());
    let h: Arc<dyn MessageHandler> = engine.clone();
    let key = SessionKey::new("mock", "u-del");

    // Create a schedule
    h.handle(make_msg(key.clone(), "/schedule every 1h \"ping\""))
        .await;
    drain_until(&platform, |r| {
        r.iter().any(|x| x.text.contains("scheduled"))
    })
    .await;

    // Get the ID from the confirmation
    let entries = sched.list(&key).await;
    assert_eq!(entries.len(), 1);
    let id = &entries[0].id;
    let short_id = &id[..8.min(id.len())];

    // Delete it
    h.handle(make_msg(
        key.clone(),
        &format!("/schedule-delete {short_id}"),
    ))
    .await;
    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("deleted"))).await;

    // Verify empty
    let entries = sched.list(&key).await;
    assert!(entries.is_empty());

    engine.shutdown().await;
}

#[tokio::test]
async fn schedule_shows_usage_without_args() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let (engine, _sched) = make_engine_with_scheduler(platform.clone());
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(SessionKey::new("mock", "u-help"), "/schedule"))
        .await;
    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("usage:"))).await;
    let replies = platform.replies().await;
    assert!(replies.iter().any(|r| r.text.contains("usage:")));

    engine.shutdown().await;
}

#[tokio::test]
async fn schedule_list_empty() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let (engine, _sched) = make_engine_with_scheduler(platform.clone());
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(
        SessionKey::new("mock", "u-empty"),
        "/schedule-list",
    ))
    .await;
    drain_until(&platform, |r| {
        r.iter().any(|x| x.text.contains("no scheduled"))
    })
    .await;
    let replies = platform.replies().await;
    assert!(replies.iter().any(|r| r.text.contains("no scheduled")));

    engine.shutdown().await;
}

#[tokio::test]
async fn schedule_invalid_format_reports_error() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let (engine, _sched) = make_engine_with_scheduler(platform.clone());
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(
        SessionKey::new("mock", "u-bad"),
        "/schedule tomorrow \"test\"",
    ))
    .await;
    drain_until(&platform, |r| {
        r.iter().any(|x| x.text.contains("invalid schedule"))
    })
    .await;
    let replies = platform.replies().await;
    assert!(replies.iter().any(|r| r.text.contains("invalid schedule")));

    engine.shutdown().await;
}

#[tokio::test]
async fn help_lists_schedule_commands() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let (engine, _sched) = make_engine_with_scheduler(platform.clone());
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(SessionKey::new("mock", "u-h2"), "/help"))
        .await;
    drain_until(&platform, |r| {
        r.iter().any(|x| x.text.contains("/schedule"))
    })
    .await;
    let replies = platform.replies().await;
    let body = &replies
        .iter()
        .find(|r| r.text.contains("/schedule"))
        .unwrap()
        .text;
    assert!(body.contains("/schedule-list"));
    assert!(body.contains("/schedule-delete"));

    engine.shutdown().await;
}
