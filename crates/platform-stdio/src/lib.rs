//! Local stdio platform: read prompts from stdin, write replies to stdout.
//! Useful for development and CI without needing LINE/Slack credentials.

use std::sync::Arc;

use async_trait::async_trait;
use core_traits::{Attachment, Message, MessageHandler, Platform, ReplyCtx, Result, SessionKey};
use tokio::io::{AsyncBufReadExt, BufReader};

pub struct StdioPlatform {
    pub user: String,
}

impl StdioPlatform {
    pub fn new() -> Self {
        Self {
            user: "local".into(),
        }
    }
}

impl Default for StdioPlatform {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Platform for StdioPlatform {
    fn name(&self) -> &'static str {
        "stdio"
    }

    async fn start(&self, handler: Arc<dyn MessageHandler>) -> Result<()> {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin).lines();
        let key = SessionKey::new("stdio", &self.user);
        println!("stdio platform ready. Type messages (Ctrl-D to exit).");
        while let Some(line) = reader.next_line().await? {
            let msg = Message {
                key: key.clone(),
                text: line,
                attachments: vec![],
                reply_ctx: ReplyCtx::default(),
                timestamp_ms: 0,
            };
            handler.handle(msg).await;
        }
        Ok(())
    }

    async fn reply(&self, _ctx: &ReplyCtx, text: &str) -> Result<()> {
        println!("{text}");
        Ok(())
    }

    async fn send_attachment(&self, _ctx: &ReplyCtx, att: &Attachment) -> Result<()> {
        println!("[attachment: {}]", att.path.display());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_name() {
        let p = StdioPlatform::new();
        assert_eq!(p.name(), "stdio");
    }

    #[test]
    fn default_user_is_local() {
        let p = StdioPlatform::new();
        assert_eq!(p.user, "local");
    }

    #[test]
    fn default_trait_impl() {
        let p = StdioPlatform::default();
        assert_eq!(p.user, "local");
    }

    #[tokio::test]
    async fn reply_succeeds() {
        let p = StdioPlatform::new();
        let ctx = ReplyCtx::default();
        let result = p.reply(&ctx, "hello");
        assert!(result.await.is_ok());
    }

    #[tokio::test]
    async fn send_attachment_succeeds() {
        let p = StdioPlatform::new();
        let att = Attachment {
            kind: core_traits::AttachmentKind::File,
            path: "/tmp/test.txt".into(),
            mime: "text/plain".into(),
            bytes: Some(5),
            name: Some("test.txt".into()),
        };
        let ctx = ReplyCtx::default();
        let result = p.send_attachment(&ctx, &att);
        assert!(result.await.is_ok());
    }
}
