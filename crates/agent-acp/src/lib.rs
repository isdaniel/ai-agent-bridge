//! ACP (Agent Client Protocol) client over JSON-RPC stdio.
//!
//! Spawns an ACP-compatible binary, performs the `initialize` handshake,
//! then opens / resumes a `session` and exposes it as a [`core_traits::AgentSession`].
//!
//! Spec reference: <https://github.com/zed-industries/agent-client-protocol>
//! (pre-1.0; we pin `protocolVersion: 1`).

pub mod jsonrpc;
pub mod protocol;
pub mod session;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use core_traits::{Agent, AgentSession, Result, SessionKey};

#[derive(Clone, Debug, Default)]
pub struct AcpConfig {
    pub binary: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
}

pub struct AcpAgent {
    cfg: Arc<AcpConfig>,
}

impl AcpAgent {
    pub fn new(cfg: AcpConfig) -> Self {
        Self { cfg: Arc::new(cfg) }
    }
}

#[async_trait]
impl Agent for AcpAgent {
    fn name(&self) -> &'static str {
        "acp"
    }
    async fn start_session(
        &self,
        key: SessionKey,
        resume: Option<String>,
    ) -> Result<Box<dyn AgentSession>> {
        session::AcpSession::spawn(self.cfg.clone(), key, resume)
            .await
            .map(|s| Box::new(s) as Box<dyn AgentSession>)
    }
}
