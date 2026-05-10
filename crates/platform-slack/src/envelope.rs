//! Slack Socket Mode envelope parsing.
//!
//! Socket Mode wraps every event in an envelope:
//! ```json
//! { "envelope_id": "...", "type": "events_api", "payload": { ... } }
//! ```
//! We only care about `events_api` envelopes carrying `message` events.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Envelope {
    pub envelope_id: Option<String>,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct EventsApiPayload {
    #[allow(dead_code)]
    pub team_id: Option<String>,
    pub event: SlackEvent,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SlackEvent {
    Message(MessageEvent),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub struct MessageEvent {
    pub channel: Option<String>,
    pub user: Option<String>,
    #[serde(default)]
    pub text: String,
    pub ts: Option<String>,
    pub thread_ts: Option<String>,
    /// Bot messages have `bot_id` set; we ignore them to avoid loops.
    pub bot_id: Option<String>,
    pub subtype: Option<String>,
    #[serde(default)]
    pub files: Vec<SlackFile>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SlackFile {
    pub id: String,
    pub name: Option<String>,
    pub mimetype: Option<String>,
    pub url_private_download: Option<String>,
    #[allow(dead_code)]
    pub size: Option<u64>,
}

impl MessageEvent {
    /// True for messages we should ignore (bot loops, edits, deletes, joins).
    pub fn is_skippable(&self) -> bool {
        if self.bot_id.is_some() {
            return true;
        }
        !matches!(self.subtype.as_deref(), None | Some("file_share"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_events_api_envelope() {
        let raw = r#"{
            "envelope_id": "e1",
            "type": "events_api",
            "payload": {
                "team_id": "T1",
                "event": {
                    "type": "message",
                    "channel": "C1",
                    "user": "U1",
                    "text": "hi",
                    "ts": "1700000000.000100"
                }
            }
        }"#;
        let env: Envelope = serde_json::from_str(raw).unwrap();
        assert_eq!(env.kind, "events_api");
        let p: EventsApiPayload = serde_json::from_value(env.payload).unwrap();
        match p.event {
            SlackEvent::Message(m) => {
                assert_eq!(m.text, "hi");
                assert_eq!(m.channel.as_deref(), Some("C1"));
                assert!(!m.is_skippable());
            }
            _ => panic!("expected message"),
        }
    }

    #[test]
    fn skips_bot_messages_and_edits() {
        let bot = MessageEvent {
            channel: Some("C".into()),
            user: None,
            text: "x".into(),
            ts: None,
            thread_ts: None,
            bot_id: Some("B1".into()),
            subtype: None,
            files: vec![],
        };
        assert!(bot.is_skippable());
        let edit = MessageEvent {
            channel: Some("C".into()),
            user: Some("U".into()),
            text: "x".into(),
            ts: None,
            thread_ts: None,
            bot_id: None,
            subtype: Some("message_changed".into()),
            files: vec![],
        };
        assert!(edit.is_skippable());
    }

    #[test]
    fn parses_file_share() {
        let raw = r#"{
            "type": "message",
            "subtype": "file_share",
            "channel": "C1",
            "user": "U1",
            "text": "look",
            "files": [{
                "id": "F1",
                "name": "a.png",
                "mimetype": "image/png",
                "url_private_download": "https://files.slack.com/x",
                "size": 1234
            }]
        }"#;
        let m: MessageEvent = serde_json::from_str(raw).unwrap();
        assert!(!m.is_skippable());
        assert_eq!(m.files.len(), 1);
        assert_eq!(m.files[0].mimetype.as_deref(), Some("image/png"));
    }

    #[test]
    fn ignores_non_events_api_envelopes() {
        let raw = r#"{"type":"hello","num_connections":1}"#;
        let env: Envelope = serde_json::from_str(raw).unwrap();
        assert_eq!(env.kind, "hello");
    }
}
