//! End-to-end engine wiring: a chat message flows in, the EchoAgent answers,
//! the MockPlatform records the reply.

use std::sync::Arc;

use core_engine::Engine;
use core_traits::{Message, MessageHandler, ReplyCtx, SessionKey};
use test_support::{EchoAgent, MockPlatform};

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
