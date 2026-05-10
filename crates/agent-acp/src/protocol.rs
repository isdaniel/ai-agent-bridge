//! ACP message types — only the subset we currently use, kept liberal with
//! `#[serde(default)]` so future spec additions don't break parsing.
//!
//! Reference: <https://github.com/zed-industries/agent-client-protocol>

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
pub struct InitializeParams<'a> {
    #[serde(rename = "protocolVersion")]
    pub protocol_version: u32,
    #[serde(rename = "clientCapabilities")]
    pub client_capabilities: ClientCapabilities<'a>,
}

#[derive(Debug, Serialize)]
pub struct ClientCapabilities<'a> {
    pub fs: FsCapabilities,
    #[serde(rename = "clientInfo")]
    pub client_info: ClientInfo<'a>,
}

#[derive(Debug, Serialize)]
pub struct FsCapabilities {
    #[serde(rename = "readTextFile")]
    pub read_text_file: bool,
    #[serde(rename = "writeTextFile")]
    pub write_text_file: bool,
}

#[derive(Debug, Serialize)]
pub struct ClientInfo<'a> {
    pub name: &'a str,
    pub version: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct InitializeResult {
    #[serde(rename = "protocolVersion", default)]
    pub protocol_version: u32,
    #[serde(rename = "agentCapabilities", default)]
    pub agent_capabilities: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct SessionNewParams<'a> {
    pub cwd: &'a str,
    #[serde(rename = "mcpServers")]
    pub mcp_servers: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct SessionNewResult {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct PromptParams<'a> {
    #[serde(rename = "sessionId")]
    pub session_id: &'a str,
    pub prompt: Vec<PromptBlock>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PromptBlock {
    Text { text: String },
    Image { source: ImageSource },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageSource {
    Base64 {
        #[serde(rename = "mediaType")]
        media_type: String,
        data: String,
    },
    Path {
        path: String,
        #[serde(rename = "mediaType")]
        media_type: String,
    },
}

/// Notification body of `session/update`.
#[derive(Debug, Deserialize)]
pub struct SessionUpdate {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub update: UpdateBody,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UpdateBody {
    AgentMessageChunk {
        content: ContentBlock,
    },
    ToolCall {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(default)]
        title: Option<String>,
        #[serde(default, rename = "toolKind")]
        tool_kind: Option<String>,
    },
    ToolCallUpdate {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(default)]
        status: Option<String>,
    },
    Plan {
        #[serde(default)]
        entries: serde_json::Value,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    #[serde(other)]
    Other,
}

/// Permission request notification (`session/request_permission`).
#[derive(Debug, Deserialize)]
pub struct PermissionRequestParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "requestId")]
    pub request_id: String,
    #[serde(rename = "toolCall", default)]
    pub tool_call: serde_json::Value,
    #[serde(default)]
    pub options: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_session_update_text_chunk() {
        let raw = r#"{
            "sessionId":"s1",
            "update":{"kind":"agent_message_chunk","content":{"type":"text","text":"hi"}}
        }"#;
        let u: SessionUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(u.session_id, "s1");
        match u.update {
            UpdateBody::AgentMessageChunk { content } => match content {
                ContentBlock::Text { text } => assert_eq!(text, "hi"),
                _ => panic!("content"),
            },
            _ => panic!("kind"),
        }
    }

    #[test]
    fn parse_tool_call() {
        let raw = r#"{
            "sessionId":"s1",
            "update":{"kind":"tool_call","toolCallId":"t1","title":"Read"}
        }"#;
        let u: SessionUpdate = serde_json::from_str(raw).unwrap();
        assert!(matches!(u.update, UpdateBody::ToolCall { .. }));
    }

    #[test]
    fn unknown_update_kind_falls_through() {
        let raw = r#"{"sessionId":"s","update":{"kind":"futuristic"}}"#;
        let u: SessionUpdate = serde_json::from_str(raw).unwrap();
        assert!(matches!(u.update, UpdateBody::Other));
    }
}
