//! Test helpers: mock Agent / Platform implementations for unit tests across
//! the workspace.

use std::sync::Arc;

use async_trait::async_trait;
use core_traits::{
    Agent, AgentSession, Attachment, Event, MessageHandler, Platform, ReplyCtx, Result, SessionKey,
};
use tokio::sync::{mpsc, Mutex};

#[derive(Default, Clone)]
pub struct RecordedReply {
    pub ctx: ReplyCtx,
    pub text: String,
}

pub struct MockPlatform {
    pub name: &'static str,
    pub replies: Arc<Mutex<Vec<RecordedReply>>>,
}

impl MockPlatform {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            replies: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub async fn replies(&self) -> Vec<RecordedReply> {
        self.replies.lock().await.clone()
    }
}

#[async_trait]
impl Platform for MockPlatform {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn start(&self, _h: Arc<dyn MessageHandler>) -> Result<()> {
        Ok(())
    }
    async fn reply(&self, ctx: &ReplyCtx, text: &str) -> Result<()> {
        self.replies.lock().await.push(RecordedReply {
            ctx: ctx.clone(),
            text: text.to_string(),
        });
        Ok(())
    }
    async fn send_attachment(&self, _ctx: &ReplyCtx, _att: &Attachment) -> Result<()> {
        Ok(())
    }
}

pub struct EchoAgent;

#[async_trait]
impl Agent for EchoAgent {
    fn name(&self) -> &'static str {
        "echo"
    }
    async fn start_session(
        &self,
        _key: SessionKey,
        _resume: Option<String>,
    ) -> Result<Box<dyn AgentSession>> {
        Ok(Box::new(EchoSession {
            id: "echo-1".into(),
            tx: None,
        }))
    }
}

pub struct EchoSession {
    id: String,
    tx: Option<mpsc::Sender<Event>>,
}

#[async_trait]
impl AgentSession for EchoSession {
    fn id(&self) -> String {
        self.id.clone()
    }
    async fn send(&mut self, prompt: String, _atts: Vec<Attachment>) -> Result<()> {
        if let Some(tx) = &self.tx {
            tx.send(Event::AssistantText {
                text: format!("echo: {prompt}"),
                partial: false,
            })
            .await
            .ok();
            tx.send(Event::Done {
                session_id: self.id.clone(),
            })
            .await
            .ok();
        }
        Ok(())
    }
    fn events(&mut self) -> mpsc::Receiver<Event> {
        let (tx, rx) = mpsc::channel(8);
        self.tx = Some(tx);
        rx
    }
    async fn answer_permission(&mut self, _id: String, _allow: bool) -> Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
