//! SQLite-backed state store for sessions and schedules.
//!
//! Uses WAL-mode SQLite for O(1) per-row writes. Each mutation is an
//! immediate write — no separate `persist()` step needed.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use core_traits::SessionKey;
use rusqlite::{params, Connection, OptionalExtension};
use tracing::warn;

use crate::registry::RegistryEntry;
use crate::scheduler::{ScheduleEntry, ScheduleKind};

const SCHEMA_VERSION: u32 = 1;

pub struct StateDb {
    conn: Connection,
}

impl StateDb {
    pub fn open(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn = Connection::open(&path)?;
        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    pub fn in_memory() -> Self {
        let conn = Connection::open_in_memory().expect("in-memory SQLite");
        let db = Self { conn };
        db.init_schema().expect("schema init");
        db
    }

    fn init_schema(&self) -> Result<()> {
        let version: u32 = self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))?;

        if version == 0 {
            self.conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 CREATE TABLE IF NOT EXISTS sessions (
                     key                    TEXT PRIMARY KEY,
                     agent                  TEXT NOT NULL DEFAULT '',
                     active_session_id      TEXT,
                     past_agent_session_ids TEXT NOT NULL DEFAULT '[]'
                 );
                 CREATE TABLE IF NOT EXISTS schedules (
                     id              TEXT PRIMARY KEY,
                     session_key     TEXT NOT NULL,
                     prompt          TEXT NOT NULL,
                     reply_ctx       TEXT NOT NULL,
                     schedule_kind   TEXT NOT NULL,
                     created_at_ms   INTEGER NOT NULL,
                     last_fired_ms   INTEGER
                 );
                 CREATE INDEX IF NOT EXISTS idx_schedules_key ON schedules(session_key);
                 PRAGMA user_version = 1;",
            )?;
        } else if version > SCHEMA_VERSION {
            warn!(
                found = version,
                expected = SCHEMA_VERSION,
                "state.db schema from the future"
            );
            return Err(anyhow!(
                "state.db user_version {version} > expected {SCHEMA_VERSION}"
            ));
        }

        Ok(())
    }

    // ── Session operations ──────────────────────────────────────────────

    pub fn get_session(&self, key: &SessionKey) -> Option<RegistryEntry> {
        self.conn
            .query_row(
                "SELECT agent, active_session_id, past_agent_session_ids
                 FROM sessions WHERE key = ?1",
                params![key.0],
                |row| {
                    let agent: String = row.get(0)?;
                    let active: Option<String> = row.get(1)?;
                    let past_json: String = row.get(2)?;
                    let past: Vec<(String, String)> =
                        serde_json::from_str(&past_json).unwrap_or_default();
                    Ok(RegistryEntry {
                        agent,
                        active_session_id: active,
                        past_agent_session_ids: past,
                    })
                },
            )
            .optional()
            .ok()
            .flatten()
    }

    pub fn upsert_session(&self, key: &SessionKey, entry: &RegistryEntry) -> Result<()> {
        let past_json = serde_json::to_string(&entry.past_agent_session_ids)?;
        self.conn.execute(
            "INSERT INTO sessions (key, agent, active_session_id, past_agent_session_ids)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(key) DO UPDATE SET
                 agent = excluded.agent,
                 active_session_id = excluded.active_session_id,
                 past_agent_session_ids = excluded.past_agent_session_ids",
            params![key.0, entry.agent, entry.active_session_id, past_json],
        )?;
        Ok(())
    }

    pub fn remove_session(&self, key: &SessionKey) -> Result<()> {
        self.conn
            .execute("DELETE FROM sessions WHERE key = ?1", params![key.0])?;
        Ok(())
    }

    pub fn all_sessions(&self) -> HashMap<SessionKey, RegistryEntry> {
        let mut stmt = self
            .conn
            .prepare("SELECT key, agent, active_session_id, past_agent_session_ids FROM sessions")
            .expect("prepare all_sessions");
        let iter = stmt
            .query_map([], |row| {
                let key: String = row.get(0)?;
                let agent: String = row.get(1)?;
                let active: Option<String> = row.get(2)?;
                let past_json: String = row.get(3)?;
                let past: Vec<(String, String)> =
                    serde_json::from_str(&past_json).unwrap_or_default();
                Ok((
                    SessionKey(key),
                    RegistryEntry {
                        agent,
                        active_session_id: active,
                        past_agent_session_ids: past,
                    },
                ))
            })
            .expect("query all_sessions");
        iter.filter_map(|r| r.ok()).collect()
    }

    // ── Schedule operations ─────────────────────────────────────────────

    pub fn add_schedule(&self, entry: &ScheduleEntry) -> Result<()> {
        let reply_ctx_json = serde_json::to_string(&entry.reply_ctx)?;
        let kind_json = serde_json::to_string(&entry.schedule)?;
        self.conn.execute(
            "INSERT INTO schedules (id, session_key, prompt, reply_ctx, schedule_kind, created_at_ms, last_fired_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                entry.id,
                entry.key.0,
                entry.prompt,
                reply_ctx_json,
                kind_json,
                entry.created_at_ms,
                entry.last_fired_ms,
            ],
        )?;
        Ok(())
    }

    pub fn remove_schedule(&self, key: &SessionKey, id: &str) -> Result<bool> {
        let n = self.conn.execute(
            "DELETE FROM schedules WHERE session_key = ?1 AND id = ?2",
            params![key.0, id],
        )?;
        Ok(n > 0)
    }

    pub fn list_schedules(&self, key: &SessionKey) -> Vec<ScheduleEntry> {
        self.query_schedules(Some(key))
    }

    pub fn list_all_schedules(&self) -> Vec<ScheduleEntry> {
        self.query_schedules(None)
    }

    fn query_schedules(&self, key: Option<&SessionKey>) -> Vec<ScheduleEntry> {
        let (sql, key_val);
        match key {
            Some(k) => {
                sql = "SELECT id, session_key, prompt, reply_ctx, schedule_kind, created_at_ms, last_fired_ms
                       FROM schedules WHERE session_key = ?1";
                key_val = Some(k.0.clone());
            }
            None => {
                sql = "SELECT id, session_key, prompt, reply_ctx, schedule_kind, created_at_ms, last_fired_ms
                       FROM schedules";
                key_val = None;
            }
        }

        let mut stmt = match self.conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let map_row = |row: &rusqlite::Row| -> rusqlite::Result<Option<ScheduleEntry>> {
            let id: String = row.get(0)?;
            let session_key: String = row.get(1)?;
            let prompt: String = row.get(2)?;
            let reply_ctx_json: String = row.get(3)?;
            let kind_json: String = row.get(4)?;
            let created_at_ms: i64 = row.get(5)?;
            let last_fired_ms: Option<i64> = row.get(6)?;

            let reply_ctx = match serde_json::from_str(&reply_ctx_json) {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            let schedule: ScheduleKind = match serde_json::from_str(&kind_json) {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };

            Ok(Some(ScheduleEntry {
                id,
                key: SessionKey(session_key),
                prompt,
                reply_ctx,
                schedule,
                created_at_ms,
                last_fired_ms,
            }))
        };

        let rows = if let Some(ref kv) = key_val {
            stmt.query_map(params![kv], map_row)
        } else {
            stmt.query_map([], map_row)
        };

        match rows {
            Ok(iter) => iter.filter_map(|r| r.ok().flatten()).collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn update_schedule_fired(
        &self,
        id: &str,
        last_fired_ms: i64,
        new_kind: &ScheduleKind,
    ) -> Result<()> {
        let kind_json = serde_json::to_string(new_kind)?;
        self.conn.execute(
            "UPDATE schedules SET last_fired_ms = ?1, schedule_kind = ?2 WHERE id = ?3",
            params![last_fired_ms, kind_json, id],
        )?;
        Ok(())
    }

    pub fn remove_schedules_by_ids(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            "DELETE FROM schedules WHERE id IN ({})",
            placeholders.join(",")
        );
        let params: Vec<&dyn rusqlite::types::ToSql> = ids
            .iter()
            .map(|s| s as &dyn rusqlite::types::ToSql)
            .collect();
        self.conn.execute(&sql, params.as_slice())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_traits::ReplyCtx;

    #[test]
    fn session_crud() {
        let db = StateDb::in_memory();
        let key = SessionKey::new("line", "U1");

        assert!(db.get_session(&key).is_none());

        let entry = RegistryEntry {
            agent: "claude".into(),
            active_session_id: Some("sess-1".into()),
            past_agent_session_ids: vec![],
        };
        db.upsert_session(&key, &entry).unwrap();

        let got = db.get_session(&key).unwrap();
        assert_eq!(got.agent, "claude");
        assert_eq!(got.active_session_id.as_deref(), Some("sess-1"));

        let all = db.all_sessions();
        assert_eq!(all.len(), 1);

        db.remove_session(&key).unwrap();
        assert!(db.get_session(&key).is_none());
    }

    #[test]
    fn session_upsert_overwrites() {
        let db = StateDb::in_memory();
        let key = SessionKey::new("line", "U1");

        let e1 = RegistryEntry {
            agent: "claude".into(),
            active_session_id: Some("s1".into()),
            past_agent_session_ids: vec![],
        };
        db.upsert_session(&key, &e1).unwrap();

        let e2 = RegistryEntry {
            agent: "copilot".into(),
            active_session_id: Some("s2".into()),
            past_agent_session_ids: vec![("claude".into(), "s1".into())],
        };
        db.upsert_session(&key, &e2).unwrap();

        let got = db.get_session(&key).unwrap();
        assert_eq!(got.agent, "copilot");
        assert_eq!(got.past_agent_session_ids.len(), 1);
    }

    fn make_schedule_entry(id: &str, key: &SessionKey, prompt: &str) -> ScheduleEntry {
        ScheduleEntry {
            id: id.into(),
            key: key.clone(),
            prompt: prompt.into(),
            reply_ctx: ReplyCtx::default(),
            schedule: ScheduleKind::Recurring {
                interval_ms: 3_600_000,
                next_fire_ms: core_traits::now_ms() + 3_600_000,
            },
            created_at_ms: core_traits::now_ms(),
            last_fired_ms: None,
        }
    }

    #[test]
    fn schedule_crud() {
        let db = StateDb::in_memory();
        let key = SessionKey::new("t", "u1");

        db.add_schedule(&make_schedule_entry("s1", &key, "hello"))
            .unwrap();
        db.add_schedule(&make_schedule_entry("s2", &key, "world"))
            .unwrap();

        let list = db.list_schedules(&key);
        assert_eq!(list.len(), 2);

        let all = db.list_all_schedules();
        assert_eq!(all.len(), 2);

        let removed = db.remove_schedule(&key, "s1").unwrap();
        assert!(removed);
        assert_eq!(db.list_schedules(&key).len(), 1);

        let not_found = db.remove_schedule(&key, "nonexistent").unwrap();
        assert!(!not_found);
    }

    #[test]
    fn schedule_list_filters_by_key() {
        let db = StateDb::in_memory();
        let k1 = SessionKey::new("t", "u1");
        let k2 = SessionKey::new("t", "u2");

        db.add_schedule(&make_schedule_entry("s1", &k1, "one"))
            .unwrap();
        db.add_schedule(&make_schedule_entry("s2", &k2, "two"))
            .unwrap();

        assert_eq!(db.list_schedules(&k1).len(), 1);
        assert_eq!(db.list_schedules(&k2).len(), 1);
        assert_eq!(db.list_all_schedules().len(), 2);
    }

    #[test]
    fn schedule_update_fired() {
        let db = StateDb::in_memory();
        let key = SessionKey::new("t", "u1");
        db.add_schedule(&make_schedule_entry("s1", &key, "test"))
            .unwrap();

        let new_kind = ScheduleKind::Recurring {
            interval_ms: 3_600_000,
            next_fire_ms: core_traits::now_ms() + 7_200_000,
        };
        db.update_schedule_fired("s1", core_traits::now_ms(), &new_kind)
            .unwrap();

        let list = db.list_schedules(&key);
        assert_eq!(list.len(), 1);
        assert!(list[0].last_fired_ms.is_some());
    }

    #[test]
    fn schedule_remove_batch() {
        let db = StateDb::in_memory();
        let key = SessionKey::new("t", "u1");
        db.add_schedule(&make_schedule_entry("s1", &key, "one"))
            .unwrap();
        db.add_schedule(&make_schedule_entry("s2", &key, "two"))
            .unwrap();
        db.add_schedule(&make_schedule_entry("s3", &key, "three"))
            .unwrap();

        db.remove_schedules_by_ids(&["s1".into(), "s3".into()])
            .unwrap();
        let remaining = db.list_all_schedules();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, "s2");
    }

    #[test]
    fn open_creates_file_and_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = StateDb::open(path.clone()).unwrap();
        let key = SessionKey::new("test", "u1");
        let entry = RegistryEntry {
            agent: "claude".into(),
            active_session_id: Some("s1".into()),
            past_agent_session_ids: vec![],
        };
        db.upsert_session(&key, &entry).unwrap();
        drop(db);

        let db2 = StateDb::open(path).unwrap();
        let got = db2.get_session(&key).unwrap();
        assert_eq!(got.agent, "claude");
    }
}
