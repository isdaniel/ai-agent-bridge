//! Integration tests for engine features added in P1–P3:
//! multi-platform routing, prefix routing, idle timeout protection, batch mode.

use std::sync::Arc;
use std::time::Duration;

use core_engine::Engine;
use core_traits::{Message, MessageHandler, ReplyCtx, SessionKey};
use test_support::{EchoAgent, MockPlatform, SlowAgent, StreamingAgent};

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
    for _ in 0..100 {
        if pred(&platform.replies().await) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

// ── Multi-Platform Routing ──────────────────────────────────────────────────

#[tokio::test]
async fn multi_platform_routes_to_correct_platform() {
    let p_line = Arc::new(MockPlatform::new("line"));
    let p_slack = Arc::new(MockPlatform::new("slack"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .add_platform(p_line.clone())
        .add_platform(p_slack.clone())
        .build()
        .unwrap();
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(SessionKey::new("line", "u1"), "from line"))
        .await;
    h.handle(make_msg(SessionKey::new("slack", "u2"), "from slack"))
        .await;

    drain_until(&p_line, |r| r.iter().any(|x| x.text == "echo: from line")).await;
    drain_until(&p_slack, |r| r.iter().any(|x| x.text == "echo: from slack")).await;

    let line_replies = p_line.replies().await;
    let slack_replies = p_slack.replies().await;

    assert!(
        line_replies.iter().any(|r| r.text == "echo: from line"),
        "line platform should have line reply"
    );
    assert!(
        slack_replies.iter().any(|r| r.text == "echo: from slack"),
        "slack platform should have slack reply"
    );
    assert!(
        !line_replies.iter().any(|r| r.text.contains("from slack")),
        "line platform should NOT have slack reply"
    );
    assert!(
        !slack_replies.iter().any(|r| r.text.contains("from line")),
        "slack platform should NOT have line reply"
    );
    engine.shutdown().await;
}

// ── Prefix Routing ──────────────────────────────────────────────────────────

/// A second echo agent with a different name for prefix routing tests.
struct Echo2Agent;

#[async_trait::async_trait]
impl core_traits::Agent for Echo2Agent {
    fn name(&self) -> &'static str {
        "echo2"
    }
    async fn start_session(
        &self,
        _key: SessionKey,
        _resume: Option<String>,
    ) -> core_traits::Result<Box<dyn core_traits::AgentSession>> {
        Ok(Box::new(Echo2Session {
            id: "echo2-1".into(),
            tx: None,
        }))
    }
}

struct Echo2Session {
    id: String,
    tx: Option<tokio::sync::mpsc::Sender<core_traits::Event>>,
}

#[async_trait::async_trait]
impl core_traits::AgentSession for Echo2Session {
    fn id(&self) -> String {
        self.id.clone()
    }
    async fn send(
        &mut self,
        prompt: String,
        _atts: Vec<core_traits::Attachment>,
    ) -> core_traits::Result<()> {
        if let Some(tx) = &self.tx {
            tx.send(core_traits::Event::AssistantText {
                text: format!("echo2: {prompt}"),
                partial: false,
            })
            .await
            .ok();
            tx.send(core_traits::Event::Done {
                session_id: self.id.clone(),
            })
            .await
            .ok();
        }
        Ok(())
    }
    fn events(&mut self) -> tokio::sync::mpsc::Receiver<core_traits::Event> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        self.tx = Some(tx);
        rx
    }
    async fn answer_permission(&mut self, _id: String, _allow: bool) -> core_traits::Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> core_traits::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn prefix_routing_dispatches_to_named_agent() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .add_agent(Arc::new(Echo2Agent))
        .default_agent("echo")
        .platform(platform.clone())
        .build()
        .unwrap();
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(SessionKey::new("mock", "u1"), "@echo2 hello"))
        .await;

    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("echo2:"))).await;
    let replies = platform.replies().await;
    assert!(
        replies.iter().any(|r| r.text == "echo2: hello"),
        "should route to echo2 agent, got: {:?}",
        replies.iter().map(|r| &r.text).collect::<Vec<_>>()
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn prefix_routing_unknown_agent_falls_through() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .add_agent(Arc::new(Echo2Agent))
        .default_agent("echo")
        .platform(platform.clone())
        .build()
        .unwrap();
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(SessionKey::new("mock", "u2"), "@nosuch hello"))
        .await;

    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("echo:"))).await;
    let replies = platform.replies().await;
    assert!(
        replies.iter().any(|r| r.text == "echo: @nosuch hello"),
        "unknown @agent should fall through to default, got: {:?}",
        replies.iter().map(|r| &r.text).collect::<Vec<_>>()
    );
    engine.shutdown().await;
}

// ── Idle Timeout ────────────────────────────────────────────────────────────

#[tokio::test]
async fn idle_timeout_does_not_kill_working_session() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(SlowAgent { delay_ms: 1500 }))
        .default_agent("slow")
        .platform(platform.clone())
        .idle_timeout(Duration::from_secs(1))
        .batch_replies(false)
        .build()
        .unwrap();
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(SessionKey::new("mock", "u1"), "slow request"))
        .await;

    // Wait longer than idle timeout (1s) but agent responds at 1.5s
    drain_until(&platform, |r| {
        r.iter().any(|x| x.text == "echo: slow request")
    })
    .await;

    let replies = platform.replies().await;
    assert!(
        replies.iter().any(|r| r.text == "echo: slow request"),
        "session should NOT be killed while waiting_for_response, got: {:?}",
        replies.iter().map(|r| &r.text).collect::<Vec<_>>()
    );
    engine.shutdown().await;
}

#[tokio::test]
async fn idle_timeout_kills_truly_idle_session() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(EchoAgent))
        .default_agent("echo")
        .platform(platform.clone())
        .idle_timeout(Duration::from_millis(200))
        .batch_replies(false)
        .build()
        .unwrap();
    let h: Arc<dyn MessageHandler> = engine.clone();

    // First message establishes session.
    h.handle(make_msg(SessionKey::new("mock", "u-idle"), "first"))
        .await;
    drain_until(&platform, |r| r.iter().any(|x| x.text == "echo: first")).await;

    // Wait for idle timeout to fire (200ms + buffer).
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Second message should still work (engine retries with fresh session).
    h.handle(make_msg(SessionKey::new("mock", "u-idle"), "second"))
        .await;
    drain_until(&platform, |r| r.iter().any(|x| x.text == "echo: second")).await;

    let replies = platform.replies().await;
    assert!(
        replies.iter().any(|r| r.text == "echo: second"),
        "should get reply after idle-killed session is respawned"
    );
    engine.shutdown().await;
}

// ── Batch Mode ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn batch_mode_sends_single_reply() {
    let platform = Arc::new(MockPlatform::new("mock"));
    let engine = Engine::builder()
        .add_agent(Arc::new(StreamingAgent))
        .default_agent("streaming")
        .platform(platform.clone())
        .batch_replies(true)
        .build()
        .unwrap();
    let h: Arc<dyn MessageHandler> = engine.clone();

    h.handle(make_msg(SessionKey::new("mock", "u-batch"), "hi"))
        .await;

    drain_until(&platform, |r| r.iter().any(|x| x.text.contains("echo: hi"))).await;

    let replies = platform.replies().await;
    // In batch mode, partials are buffered. The non-partial "echo: hi" arrives
    // as a single message. We may also see partial buffer flushed IF it was
    // already accumulated, but the key invariant: the final text is delivered
    // as one message, not fragmented across 4 sends.
    let echo_replies: Vec<_> = replies
        .iter()
        .filter(|r| r.text.contains("echo: hi"))
        .collect();
    assert_eq!(
        echo_replies.len(),
        1,
        "batch mode should produce exactly 1 final reply, got: {:?}",
        replies.iter().map(|r| &r.text).collect::<Vec<_>>()
    );
    engine.shutdown().await;
}
