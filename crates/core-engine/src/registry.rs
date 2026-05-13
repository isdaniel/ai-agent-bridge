//! Persistent SessionRegistry: maps SessionKey → agent + last session_id.
//!
//! Backed by SQLite via [`StateDb`]. Live agent processes are *not*
//! persisted. On restart, the next inbound message for a key spawns a
//! fresh agent and passes `--resume <last_id>`.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use core_traits::SessionKey;
use serde::{Deserialize, Serialize};

use crate::store::StateDb;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub agent: String,
    pub active_session_id: Option<String>,
    #[serde(default)]
    pub past_agent_session_ids: Vec<(String, String)>, // (agent, session_id)
}

/// Wraps a [`StateDb`] and provides session-focused accessors.
///
/// Protected externally by `Arc<Mutex<SessionRegistry>>` — callers lock
/// before calling methods, so no internal locking is needed.
pub struct SessionRegistry {
    db: StateDb,
}

impl SessionRegistry {
    pub fn new(db: StateDb) -> Self {
        Self { db }
    }

    pub fn in_memory() -> Self {
        Self {
            db: StateDb::in_memory(),
        }
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        let db = StateDb::open(path)?;
        Ok(Self { db })
    }

    pub fn db(&self) -> &StateDb {
        &self.db
    }

    pub fn agent_for(&self, key: &SessionKey) -> Option<String> {
        self.db.get_session(key).map(|e| e.agent)
    }

    pub fn last_session_id(&self, key: &SessionKey) -> Option<String> {
        self.db.get_session(key).and_then(|e| e.active_session_id)
    }

    pub fn get_session(&self, key: &SessionKey) -> Option<RegistryEntry> {
        self.db.get_session(key)
    }

    pub fn record_session(&mut self, key: SessionKey, agent: String, session_id: String) {
        let mut entry = self.db.get_session(&key).unwrap_or_default();
        if let Some(prev_id) = entry.active_session_id.take() {
            if !entry.agent.is_empty() && (entry.agent != agent || prev_id != session_id) {
                entry
                    .past_agent_session_ids
                    .push((std::mem::take(&mut entry.agent), prev_id));
            }
        }
        entry.agent = agent;
        entry.active_session_id = Some(session_id);
        let _ = self.db.upsert_session(&key, &entry);
    }

    pub fn clear_active(&mut self, key: &SessionKey) {
        if let Some(mut entry) = self.db.get_session(key) {
            if let Some(id) = entry.active_session_id.take() {
                entry.past_agent_session_ids.push((entry.agent.clone(), id));
            }
            let _ = self.db.upsert_session(key, &entry);
        }
    }

    pub fn clear_all(&mut self, key: &SessionKey) {
        let _ = self.db.remove_session(key);
    }

    pub fn entries(&self) -> HashMap<SessionKey, RegistryEntry> {
        self.db.all_sessions()
    }

    pub fn set_agent(&mut self, key: SessionKey, agent: String) {
        let mut entry = self.db.get_session(&key).unwrap_or_default();
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
        let _ = self.db.upsert_session(&key, &entry);
    }

    /// No-op — SQLite auto-persists. Retained for API compatibility.
    pub async fn persist(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn round_trip_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        let mut reg = SessionRegistry::open(path.clone()).unwrap();
        let k = SessionKey::new("line", "U1");
        reg.record_session(k.clone(), "claude".into(), "sess-1".into());

        drop(reg);

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
        let entry = reg.get_session(&k).unwrap();
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
        let e = reg.get_session(&k).unwrap();
        assert!(e.active_session_id.is_none());
        assert_eq!(e.past_agent_session_ids.len(), 1);
    }

    #[test]
    fn clear_all_removes_entry() {
        let mut reg = SessionRegistry::in_memory();
        let k = SessionKey::new("line", "U3");
        reg.record_session(k.clone(), "claude".into(), "s1".into());
        reg.record_session(k.clone(), "copilot".into(), "s2".into());
        assert!(reg.get_session(&k).is_some());
        reg.clear_all(&k);
        assert!(reg.get_session(&k).is_none());
        assert!(reg.agent_for(&k).is_none());
        assert!(reg.last_session_id(&k).is_none());
    }

    #[test]
    fn set_agent_same_agent_noop() {
        let mut reg = SessionRegistry::in_memory();
        let k = SessionKey::new("line", "U1");
        reg.record_session(k.clone(), "claude".into(), "s1".into());
        reg.set_agent(k.clone(), "claude".into());
        let e = reg.get_session(&k).unwrap();
        assert_eq!(e.agent, "claude");
        assert_eq!(e.active_session_id.as_deref(), Some("s1"));
        assert!(e.past_agent_session_ids.is_empty());
    }

    #[test]
    fn set_agent_different_agent_archives_previous() {
        let mut reg = SessionRegistry::in_memory();
        let k = SessionKey::new("line", "U1");
        reg.record_session(k.clone(), "claude".into(), "s1".into());
        reg.set_agent(k.clone(), "copilot".into());
        let e = reg.get_session(&k).unwrap();
        assert_eq!(e.agent, "copilot");
        assert!(e.active_session_id.is_none());
        assert_eq!(e.past_agent_session_ids.len(), 1);
        assert_eq!(e.past_agent_session_ids[0], ("claude".into(), "s1".into()));
    }

    #[test]
    fn record_session_same_id_no_duplicate_history() {
        let mut reg = SessionRegistry::in_memory();
        let k = SessionKey::new("line", "U1");
        reg.record_session(k.clone(), "claude".into(), "s1".into());
        reg.record_session(k.clone(), "claude".into(), "s1".into());
        let e = reg.get_session(&k).unwrap();
        assert!(e.past_agent_session_ids.is_empty());
    }

    #[test]
    fn entries_returns_all() {
        let mut reg = SessionRegistry::in_memory();
        let k1 = SessionKey::new("line", "U1");
        let k2 = SessionKey::new("slack", "U2");
        reg.record_session(k1, "claude".into(), "s1".into());
        reg.record_session(k2, "copilot".into(), "s2".into());
        let all = reg.entries();
        assert_eq!(all.len(), 2);
    }
}
