//! Strongly-typed Claude Code stream-json events.
//!
//! Only fields we actively use are typed; the rest are kept as `serde_json::Value`
//! so the parser is forward-compatible with new Claude releases.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    /// Initial `init` system frame announcing session_id, model, etc.
    System {
        #[serde(default)]
        subtype: Option<String>,
        #[serde(default)]
        session_id: Option<String>,
        #[serde(flatten)]
        rest: serde_json::Value,
    },
    Assistant {
        message: AssistantMessage,
        #[serde(default)]
        session_id: Option<String>,
    },
    User {
        #[serde(default)]
        session_id: Option<String>,
        #[serde(flatten)]
        rest: serde_json::Value,
    },
    /// Final per-turn frame; carries final session_id.
    Result {
        #[serde(default)]
        subtype: Option<String>,
        #[serde(default)]
        session_id: Option<String>,
        #[serde(flatten)]
        rest: serde_json::Value,
    },
    /// Permission / interrupt control message from claude.
    ControlRequest {
        request_id: String,
        request: ControlReq,
    },
    /// Emitted when `--include-partial-messages` is on. Wraps an Anthropic-style
    /// raw streaming event (`message_start`, `content_block_start`,
    /// `content_block_delta`, ...). We only care about `text_delta` deltas.
    StreamEvent {
        #[serde(default)]
        session_id: Option<String>,
        event: PartialEvent,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartialEvent {
    ContentBlockDelta {
        #[serde(default)]
        index: u32,
        delta: PartialDelta,
    },
    #[serde(other)]
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartialDelta {
    TextDelta {
        text: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssistantMessage {
    #[serde(default)]
    pub content: Vec<ContentBlock>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        #[serde(default)]
        input: serde_json::Value,
    },
    ToolResult {
        tool_use_id: String,
        #[serde(default)]
        content: serde_json::Value,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "subtype", rename_all = "snake_case")]
pub enum ControlReq {
    PermissionRequest {
        tool_name: String,
        #[serde(default)]
        description: String,
        #[serde(default)]
        input: serde_json::Value,
    },
    Interrupt,
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_system_init() {
        let raw = r#"{"type":"system","subtype":"init","session_id":"abc","model":"sonnet"}"#;
        let evt: StreamEvent = serde_json::from_str(raw).unwrap();
        match evt {
            StreamEvent::System { session_id, .. } => {
                assert_eq!(session_id.as_deref(), Some("abc"))
            }
            _ => panic!("variant"),
        }
    }

    #[test]
    fn parses_assistant_text() {
        let raw = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#;
        let evt: StreamEvent = serde_json::from_str(raw).unwrap();
        match evt {
            StreamEvent::Assistant { message, .. } => match &message.content[0] {
                ContentBlock::Text { text } => assert_eq!(text, "hi"),
                _ => panic!("block"),
            },
            _ => panic!("variant"),
        }
    }

    #[test]
    fn parses_control_permission() {
        let raw = r#"{"type":"control_request","request_id":"r1","request":{"subtype":"permission_request","tool_name":"Bash","description":"ls","input":{}}}"#;
        let evt: StreamEvent = serde_json::from_str(raw).unwrap();
        match evt {
            StreamEvent::ControlRequest {
                request_id,
                request,
            } => {
                assert_eq!(request_id, "r1");
                match request {
                    ControlReq::PermissionRequest { tool_name, .. } => {
                        assert_eq!(tool_name, "Bash")
                    }
                    _ => panic!("subtype"),
                }
            }
            _ => panic!("variant"),
        }
    }

    #[test]
    fn unknown_subtype_falls_through() {
        let raw =
            r#"{"type":"control_request","request_id":"r2","request":{"subtype":"future_thing"}}"#;
        let evt: StreamEvent = serde_json::from_str(raw).unwrap();
        if let StreamEvent::ControlRequest { request, .. } = evt {
            assert!(matches!(request, ControlReq::Unknown));
        } else {
            panic!("variant")
        }
    }

    #[test]
    fn parses_partial_text_delta() {
        let raw = r#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hel"}}}"#;
        let evt: StreamEvent = serde_json::from_str(raw).unwrap();
        match evt {
            StreamEvent::StreamEvent { event, .. } => match event {
                PartialEvent::ContentBlockDelta { delta, .. } => match delta {
                    PartialDelta::TextDelta { text } => assert_eq!(text, "hel"),
                    _ => panic!("delta"),
                },
                _ => panic!("partial event"),
            },
            _ => panic!("variant"),
        }
    }

    #[test]
    fn unknown_partial_event_falls_through() {
        let raw = r#"{"type":"stream_event","event":{"type":"message_start"}}"#;
        let evt: StreamEvent = serde_json::from_str(raw).unwrap();
        if let StreamEvent::StreamEvent { event, .. } = evt {
            assert!(matches!(event, PartialEvent::Other));
        } else {
            panic!("variant");
        }
    }
}
