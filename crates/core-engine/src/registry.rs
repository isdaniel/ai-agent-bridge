//! Persistent SessionRegistry: maps SessionKey → agent + last session_id.
//!
//! Live agent processes are *not* persisted. On restart, the next inbound
//! message for a key spawns a fresh agent and passes `--resume <last_id>`.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use core_traits::SessionKey;
use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub agent: String,
    pub active_session_id: Option<String>,
    #[serde(default)]
    pub past_agent_session_ids: Vec<(String, String)>, // (agent, session_id)
}

#[derive(Serialize, Deserialize, Debug)]
struct StateFile {
    schema_version: u32,
    entries: HashMap<String, RegistryEntry>,
}

pub struct SessionRegistry {
    path: Option<PathBuf>,
    entries: HashMap<SessionKey, RegistryEntry>,
}

impl SessionRegistry {
    pub fn in_memory() -> Self {
        Self {
            path: None,
            entries: HashMap::new(),
        }
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        let mut me = Self {
            path: Some(path.clone()),
            entries: HashMap::new(),
        };
        if path.exists() {
            let raw = std::fs::read_to_string(&path)?;
            let parsed: StateFile = serde_json::from_str(&raw)?;
            if parsed.schema_version != SCHEMA_VERSION {
                let bak = path.with_extension("json.bak");
                let _ = std::fs::rename(&path, bak);
                tracing::warn!(
                    found = parsed.schema_version,
                    expected = SCHEMA_VERSION,
                    "state.json schema mismatch; renamed to .bak"
                );
            } else {
                me.entries = parsed
                    .entries
                    .into_iter()
                    .map(|(k, v)| (SessionKey(k), v))
                    .collect();
            }
        }
        Ok(me)
    }

    pub fn agent_for(&self, key: &SessionKey) -> Option<String> {
        self.entries.get(key).map(|e| e.agent.clone())
    }

    pub fn last_session_id(&self, key: &SessionKey) -> Option<String> {
        self.entries
            .get(key)
            .and_then(|e| e.active_session_id.clone())
    }

    pub fn record_session(&mut self, key: SessionKey, agent: String, session_id: String) {
        let entry = self.entries.entry(key).or_default();
        // Switching agent or session — push prior to history.
        if let Some(prev_id) = entry.active_session_id.take() {
            if !entry.agent.is_empty() && (entry.agent != agent || prev_id != session_id) {
                entry
                    .past_agent_session_ids
                    .push((std::mem::take(&mut entry.agent), prev_id));
            }
        }
        entry.agent = agent;
        entry.active_session_id = Some(session_id);
    }

    pub fn clear_active(&mut self, key: &SessionKey) {
        if let Some(entry) = self.entries.get_mut(key) {
            if let Some(id) = entry.active_session_id.take() {
                entry.past_agent_session_ids.push((entry.agent.clone(), id));
            }
        }
    }

    /// Remove the entire entry for `key` — active session, all history, and
    /// agent binding. The next inbound message will start completely fresh
    /// with the default agent and a brand-new session id.
    pub fn clear_all(&mut self, key: &SessionKey) {
        self.entries.remove(key);
    }

    pub fn entries(&self) -> &HashMap<SessionKey, RegistryEntry> {
        &self.entries
    }

    /// Set or replace the active agent for `key`. If a different agent was
    /// previously active, its session_id is archived to history.
    pub fn set_agent(&mut self, key: SessionKey, agent: String) {
        let entry = self.entries.entry(key).or_default();
        if !entry.agent.is_empty() && entry.agent != agent {
            if let Some(prev_id) = entry.active_session_id.take() {
                entry
                    .past_agent_session_ids
                    .push((std::mem::take(&mut entry.agent), prev_id));
            } else {
                entry.agent.clear();
            }
        }
        entry.agent = agent;
    }

    pub async fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let snapshot = StateFile {
            schema_version: SCHEMA_VERSION,
            entries: self
                .entries
                .iter()
                .map(|(k, v)| (k.0.clone(), v.clone()))
                .collect(),
        };
        let json = serde_json::to_vec_pretty(&snapshot)?;
        let path = path.clone();
        // Atomic write via tempfile + rename, on a blocking task.
        tokio::task::spawn_blocking(move || -> Result<()> {
            let dir = path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("no parent dir"))?;
            std::fs::create_dir_all(dir)?;
            let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
            std::io::Write::write_all(&mut tmp, &json)?;
            tmp.persist(&path)
                .map_err(|e| anyhow::anyhow!("persist: {e}"))?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn round_trip_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let mut reg = SessionRegistry::open(path.clone()).unwrap();
        let k = SessionKey::new("line", "U1");
        reg.record_session(k.clone(), "claude".into(), "sess-1".into());
        reg.persist().await.unwrap();

        let reg2 = SessionRegistry::open(path).unwrap();
        assert_eq!(reg2.agent_for(&k).as_deref(), Some("claude"));
        assert_eq!(reg2.last_session_id(&k).as_deref(), Some("sess-1"));
    }

    #[test]
    fn switch_agent_pushes_history() {
        let mut reg = SessionRegistry::in_memory();
        let k = SessionKey::new("line", "U1");
        reg.record_session(k.clone(), "claude".into(), "s1".into());
        reg.record_session(k.clone(), "copilot".into(), "s2".into());
        let entry = reg.entries.get(&k).unwrap();
        assert_eq!(entry.agent, "copilot");
        assert_eq!(entry.active_session_id.as_deref(), Some("s2"));
        assert_eq!(entry.past_agent_session_ids.len(), 1);
        assert_eq!(entry.past_agent_session_ids[0].0, "claude");
    }

    #[test]
    fn clear_active_archives() {
        let mut reg = SessionRegistry::in_memory();
        let k = SessionKey::new("slack", "U2");
        reg.record_session(k.clone(), "claude".into(), "s1".into());
        reg.clear_active(&k);
        let e = reg.entries.get(&k).unwrap();
        assert!(e.active_session_id.is_none());
        assert_eq!(e.past_agent_session_ids.len(), 1);
    }

    #[test]
    fn clear_all_removes_entry() {
        let mut reg = SessionRegistry::in_memory();
        let k = SessionKey::new("line", "U3");
        reg.record_session(k.clone(), "claude".into(), "s1".into());
        reg.record_session(k.clone(), "copilot".into(), "s2".into());
        assert!(reg.entries.contains_key(&k));
        reg.clear_all(&k);
        assert!(!reg.entries.contains_key(&k));
        assert!(reg.agent_for(&k).is_none());
        assert!(reg.last_session_id(&k).is_none());
    }
}
