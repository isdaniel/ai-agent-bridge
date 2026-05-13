//! Scheduled actions: fire a stored prompt into an agent session at a
//! future time (one-shot or recurring). Persisted via SQLite through
//! [`StateDb`] so schedules survive process restarts.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use core_traits::{now_ms, Message, ReplyCtx, SessionKey};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::store::StateDb;
use crate::Engine;

const TICK_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduleEntry {
    pub id: String,
    pub key: SessionKey,
    pub prompt: String,
    pub reply_ctx: ReplyCtx,
    pub schedule: ScheduleKind,
    pub created_at_ms: i64,
    pub last_fired_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleKind {
    Once { fire_at_ms: i64 },
    Recurring { interval_ms: u64, next_fire_ms: i64 },
}

impl ScheduleKind {
    pub fn should_fire(&self, now: i64) -> bool {
        match self {
            Self::Once { fire_at_ms } => now >= *fire_at_ms,
            Self::Recurring { next_fire_ms, .. } => now >= *next_fire_ms,
        }
    }

    pub fn advance(&mut self) {
        if let Self::Recurring {
            interval_ms,
            next_fire_ms,
        } = self
        {
            *next_fire_ms += *interval_ms as i64;
        }
    }

    pub fn is_once(&self) -> bool {
        matches!(self, Self::Once { .. })
    }
}

impl ScheduleEntry {
    pub fn display_schedule(&self) -> String {
        match &self.schedule {
            ScheduleKind::Once { fire_at_ms } => {
                format!("once at {}", format_utc_ms(*fire_at_ms))
            }
            ScheduleKind::Recurring {
                interval_ms,
                next_fire_ms,
            } => {
                format!(
                    "every {} (next: {})",
                    format_duration_ms(*interval_ms),
                    format_utc_ms(*next_fire_ms),
                )
            }
        }
    }
}

// ── Time parsing ────────────────────────────────────────────────────────

/// Parse a human-readable schedule specification into a [`ScheduleKind`].
///
/// Supported formats:
/// - `in 30m` / `in 2h` / `in 1d` — one-shot relative
/// - `at 2025-03-05 14:00` / `at 14:00` — one-shot absolute (UTC)
/// - `every 30m` / `every 2h` / `every 1d` — recurring with interval
/// - `every day 09:00` — recurring daily at fixed time (UTC)
pub fn parse_schedule(input: &str) -> Result<ScheduleKind> {
    let input = input.trim();
    if let Some(rest) = input.strip_prefix("in ") {
        let ms = parse_duration_ms(rest.trim())?;
        Ok(ScheduleKind::Once {
            fire_at_ms: now_ms() + ms as i64,
        })
    } else if let Some(rest) = input.strip_prefix("at ") {
        let fire_at = parse_absolute_utc(rest.trim())?;
        Ok(ScheduleKind::Once {
            fire_at_ms: fire_at,
        })
    } else if let Some(rest) = input.strip_prefix("every ") {
        let rest = rest.trim();
        if let Some(time_part) = rest.strip_prefix("day ") {
            let (h, m) = parse_hh_mm(time_part.trim())?;
            let interval_ms = 86_400_000u64;
            let next = next_daily_fire(h, m);
            Ok(ScheduleKind::Recurring {
                interval_ms,
                next_fire_ms: next,
            })
        } else {
            let ms = parse_duration_ms(rest)?;
            Ok(ScheduleKind::Recurring {
                interval_ms: ms,
                next_fire_ms: now_ms() + ms as i64,
            })
        }
    } else {
        Err(anyhow!(
            "unrecognized schedule format. Use: in <duration>, at <time>, every <duration>"
        ))
    }
}

fn parse_duration_ms(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("empty duration"));
    }
    let (num_str, suffix) = if let Some(num) = s.strip_suffix('s') {
        (num, "s")
    } else if let Some(num) = s.strip_suffix('m') {
        (num, "m")
    } else if let Some(num) = s.strip_suffix('h') {
        (num, "h")
    } else if let Some(num) = s.strip_suffix('d') {
        (num, "d")
    } else {
        return Err(anyhow!("unknown duration suffix in `{s}`. Use s/m/h/d"));
    };
    let n: u64 = num_str
        .trim()
        .parse()
        .map_err(|_| anyhow!("invalid number in duration `{s}`"))?;
    if n == 0 {
        return Err(anyhow!("duration must be > 0"));
    }
    let ms = match suffix {
        "s" => n * 1_000,
        "m" => n * 60_000,
        "h" => n * 3_600_000,
        "d" => n * 86_400_000,
        _ => unreachable!(),
    };
    Ok(ms)
}

fn parse_hh_mm(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(anyhow!("expected HH:MM format, got `{s}`"));
    }
    let h: u32 = parts[0]
        .parse()
        .map_err(|_| anyhow!("invalid hour in `{s}`"))?;
    let m: u32 = parts[1]
        .parse()
        .map_err(|_| anyhow!("invalid minute in `{s}`"))?;
    if h >= 24 {
        return Err(anyhow!("hour must be 0-23, got {h}"));
    }
    if m >= 60 {
        return Err(anyhow!("minute must be 0-59, got {m}"));
    }
    Ok((h, m))
}

/// Parse absolute UTC time: `2025-03-05 14:00` or just `14:00` (today/tomorrow).
fn parse_absolute_utc(s: &str) -> Result<i64> {
    let s = s.trim();
    if s.contains('-') {
        let parts: Vec<&str> = s.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return Err(anyhow!("expected YYYY-MM-DD HH:MM, got `{s}`"));
        }
        let date_parts: Vec<&str> = parts[0].split('-').collect();
        if date_parts.len() != 3 {
            return Err(anyhow!("expected YYYY-MM-DD date, got `{}`", parts[0]));
        }
        let year: i64 = date_parts[0].parse().map_err(|_| anyhow!("invalid year"))?;
        let month: i64 = date_parts[1]
            .parse()
            .map_err(|_| anyhow!("invalid month"))?;
        let day: i64 = date_parts[2].parse().map_err(|_| anyhow!("invalid day"))?;
        if !(1..=12).contains(&month) {
            return Err(anyhow!("month must be 1-12"));
        }
        if !(1..=31).contains(&day) {
            return Err(anyhow!("day must be 1-31"));
        }
        let (h, m) = parse_hh_mm(parts[1])?;
        let epoch_ms = date_to_epoch_ms(year, month, day, h as i64, m as i64);
        if epoch_ms <= now_ms() {
            return Err(anyhow!("scheduled time is in the past"));
        }
        Ok(epoch_ms)
    } else {
        let (h, m) = parse_hh_mm(s)?;
        let now = now_ms();
        let today_start = now - (now % 86_400_000);
        let target = today_start + (h as i64) * 3_600_000 + (m as i64) * 60_000;
        if target > now {
            Ok(target)
        } else {
            Ok(target + 86_400_000)
        }
    }
}

/// Compute next daily fire time for HH:MM UTC.
fn next_daily_fire(h: u32, m: u32) -> i64 {
    let now = now_ms();
    let today_start = now - (now % 86_400_000);
    let target = today_start + (h as i64) * 3_600_000 + (m as i64) * 60_000;
    if target > now {
        target
    } else {
        target + 86_400_000
    }
}

/// Convert date components to Unix epoch milliseconds (UTC).
fn date_to_epoch_ms(year: i64, month: i64, day: i64, hour: i64, minute: i64) -> i64 {
    let mut y = year;
    let mut m = month;
    if m <= 2 {
        y -= 1;
        m += 9;
    } else {
        m -= 3;
    }
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    (days * 86_400 + hour * 3_600 + minute * 60) * 1_000
}

// ── Display helpers ─────────────────────────────────────────────────────

fn format_utc_ms(ms: i64) -> String {
    let total_s = ms / 1000;
    let s_in_day = total_s % 86_400;
    let days_since_epoch = total_s / 86_400;

    let h = s_in_day / 3600;
    let m = (s_in_day % 3600) / 60;

    let z = days_since_epoch + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    format!("{year:04}-{month:02}-{d:02} {h:02}:{m:02} UTC")
}

fn format_duration_ms(ms: u64) -> String {
    if ms.is_multiple_of(86_400_000) {
        let d = ms / 86_400_000;
        return if d == 1 {
            "1 day".into()
        } else {
            format!("{d} days")
        };
    }
    if ms.is_multiple_of(3_600_000) {
        let h = ms / 3_600_000;
        return if h == 1 {
            "1 hour".into()
        } else {
            format!("{h} hours")
        };
    }
    if ms.is_multiple_of(60_000) {
        let m = ms / 60_000;
        return if m == 1 {
            "1 minute".into()
        } else {
            format!("{m} minutes")
        };
    }
    let s = ms / 1000;
    if s == 1 {
        "1 second".into()
    } else {
        format!("{s} seconds")
    }
}

// ── Scheduler ───────────────────────────────────────────────────────────

pub struct Scheduler {
    db: Arc<Mutex<StateDb>>,
}

impl Scheduler {
    pub fn spawn(engine: Arc<Engine>, db: Arc<Mutex<StateDb>>) -> Arc<Self> {
        let sched = Arc::new(Self { db });

        let weak = Arc::clone(&sched);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(TICK_INTERVAL).await;
                weak.tick(&engine).await;
            }
        });

        sched
    }

    pub async fn add(&self, entry: ScheduleEntry) -> Result<()> {
        let db = self.db.lock().await;
        db.add_schedule(&entry)
    }

    pub async fn remove(&self, key: &SessionKey, id: &str) -> Result<bool> {
        let db = self.db.lock().await;
        db.remove_schedule(key, id)
    }

    pub async fn list(&self, key: &SessionKey) -> Vec<ScheduleEntry> {
        let db = self.db.lock().await;
        db.list_schedules(key)
    }

    pub async fn list_all(&self) -> Vec<ScheduleEntry> {
        let db = self.db.lock().await;
        db.list_all_schedules()
    }

    async fn tick(&self, engine: &Arc<Engine>) {
        let now = now_ms();
        let mut to_fire = Vec::new();
        let mut completed_ids = Vec::new();

        {
            let db = self.db.lock().await;
            let entries = db.list_all_schedules();
            for mut entry in entries {
                if entry.schedule.should_fire(now) {
                    to_fire.push(entry.clone());
                    entry.last_fired_ms = Some(now);
                    if entry.schedule.is_once() {
                        completed_ids.push(entry.id.clone());
                    } else {
                        entry.schedule.advance();
                        if let Err(e) = db.update_schedule_fired(&entry.id, now, &entry.schedule) {
                            warn!(error = %e, id = %entry.id, "failed to update schedule after tick");
                        }
                    }
                }
            }
            if !completed_ids.is_empty() {
                if let Err(e) = db.remove_schedules_by_ids(&completed_ids) {
                    warn!(error = %e, "failed to remove completed schedules");
                }
            }
        }

        for entry in to_fire {
            debug!(
                id = %entry.id,
                key = ?entry.key,
                prompt = %entry.prompt,
                "firing scheduled action"
            );
            let msg = Message {
                key: entry.key.clone(),
                text: entry.prompt.clone(),
                attachments: vec![],
                reply_ctx: entry.reply_ctx.clone(),
                timestamp_ms: 0,
            };
            engine.dispatch(msg).await;
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_in_30m() {
        let kind = parse_schedule("in 30m").unwrap();
        match kind {
            ScheduleKind::Once { fire_at_ms } => {
                let diff = fire_at_ms - now_ms();
                assert!(diff > 29 * 60_000 && diff <= 30 * 60_000 + 500);
            }
            _ => panic!("expected Once"),
        }
    }

    #[test]
    fn parse_in_2h() {
        let kind = parse_schedule("in 2h").unwrap();
        match kind {
            ScheduleKind::Once { fire_at_ms } => {
                let diff = fire_at_ms - now_ms();
                assert!(diff > 7_000_000 && diff <= 7_200_500);
            }
            _ => panic!("expected Once"),
        }
    }

    #[test]
    fn parse_in_1d() {
        let kind = parse_schedule("in 1d").unwrap();
        match kind {
            ScheduleKind::Once { fire_at_ms } => {
                let diff = fire_at_ms - now_ms();
                assert!(diff > 86_000_000 && diff <= 86_400_500);
            }
            _ => panic!("expected Once"),
        }
    }

    #[test]
    fn parse_in_90s() {
        let kind = parse_schedule("in 90s").unwrap();
        match kind {
            ScheduleKind::Once { fire_at_ms } => {
                let diff = fire_at_ms - now_ms();
                assert!(diff > 89_000 && diff <= 90_500);
            }
            _ => panic!("expected Once"),
        }
    }

    #[test]
    fn parse_every_30m() {
        let kind = parse_schedule("every 30m").unwrap();
        match kind {
            ScheduleKind::Recurring {
                interval_ms,
                next_fire_ms,
            } => {
                assert_eq!(interval_ms, 30 * 60_000);
                let diff = next_fire_ms - now_ms();
                assert!(diff > 0 && diff <= 30 * 60_000 + 500);
            }
            _ => panic!("expected Recurring"),
        }
    }

    #[test]
    fn parse_every_1h() {
        let kind = parse_schedule("every 1h").unwrap();
        match kind {
            ScheduleKind::Recurring { interval_ms, .. } => {
                assert_eq!(interval_ms, 3_600_000);
            }
            _ => panic!("expected Recurring"),
        }
    }

    #[test]
    fn parse_every_1d() {
        let kind = parse_schedule("every 1d").unwrap();
        match kind {
            ScheduleKind::Recurring { interval_ms, .. } => {
                assert_eq!(interval_ms, 86_400_000);
            }
            _ => panic!("expected Recurring"),
        }
    }

    #[test]
    fn parse_every_day_0900() {
        let kind = parse_schedule("every day 09:00").unwrap();
        match kind {
            ScheduleKind::Recurring {
                interval_ms,
                next_fire_ms,
            } => {
                assert_eq!(interval_ms, 86_400_000);
                assert!(next_fire_ms > now_ms());
            }
            _ => panic!("expected Recurring"),
        }
    }

    #[test]
    fn parse_at_future_date() {
        let kind = parse_schedule("at 2099-01-01 00:00").unwrap();
        match kind {
            ScheduleKind::Once { fire_at_ms } => {
                assert!(fire_at_ms > now_ms());
            }
            _ => panic!("expected Once"),
        }
    }

    #[test]
    fn parse_at_time_only() {
        let kind = parse_schedule("at 23:59").unwrap();
        match kind {
            ScheduleKind::Once { fire_at_ms } => {
                assert!(fire_at_ms > now_ms());
            }
            _ => panic!("expected Once"),
        }
    }

    #[test]
    fn parse_at_past_date_errors() {
        let result = parse_schedule("at 2000-01-01 00:00");
        assert!(result.is_err());
    }

    #[test]
    fn parse_invalid_format_errors() {
        assert!(parse_schedule("tomorrow 9am").is_err());
        assert!(parse_schedule("").is_err());
        assert!(parse_schedule("in").is_err());
        assert!(parse_schedule("in 0m").is_err());
        assert!(parse_schedule("in abc").is_err());
        assert!(parse_schedule("every").is_err());
    }

    #[test]
    fn parse_invalid_time_errors() {
        assert!(parse_schedule("at 25:00").is_err());
        assert!(parse_schedule("at 12:61").is_err());
        assert!(parse_schedule("every day 25:00").is_err());
    }

    #[test]
    fn schedule_kind_should_fire_once_past() {
        let kind = ScheduleKind::Once {
            fire_at_ms: now_ms() - 1000,
        };
        assert!(kind.should_fire(now_ms()));
    }

    #[test]
    fn schedule_kind_should_fire_once_future() {
        let kind = ScheduleKind::Once {
            fire_at_ms: now_ms() + 60_000,
        };
        assert!(!kind.should_fire(now_ms()));
    }

    #[test]
    fn schedule_kind_should_fire_recurring() {
        let mut kind = ScheduleKind::Recurring {
            interval_ms: 60_000,
            next_fire_ms: now_ms() - 1000,
        };
        assert!(kind.should_fire(now_ms()));
        kind.advance();
        match &kind {
            ScheduleKind::Recurring { next_fire_ms, .. } => {
                assert!(*next_fire_ms > now_ms());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn schedule_kind_is_once() {
        assert!(ScheduleKind::Once { fire_at_ms: 0 }.is_once());
        assert!(!ScheduleKind::Recurring {
            interval_ms: 1000,
            next_fire_ms: 0,
        }
        .is_once());
    }

    #[test]
    fn format_duration_ms_display() {
        assert_eq!(format_duration_ms(60_000), "1 minute");
        assert_eq!(format_duration_ms(120_000), "2 minutes");
        assert_eq!(format_duration_ms(3_600_000), "1 hour");
        assert_eq!(format_duration_ms(7_200_000), "2 hours");
        assert_eq!(format_duration_ms(86_400_000), "1 day");
        assert_eq!(format_duration_ms(172_800_000), "2 days");
        assert_eq!(format_duration_ms(90_000), "90 seconds");
    }

    #[test]
    fn format_utc_ms_known_epoch() {
        let s = format_utc_ms(1_735_689_600_000);
        assert_eq!(s, "2025-01-01 00:00 UTC");
    }

    #[test]
    fn date_to_epoch_ms_round_trips() {
        assert_eq!(date_to_epoch_ms(1970, 1, 1, 0, 0), 0);
        assert_eq!(date_to_epoch_ms(2000, 1, 1, 0, 0), 946_684_800_000);
    }

    #[test]
    fn display_schedule_once() {
        let entry = ScheduleEntry {
            id: "test".into(),
            key: SessionKey::new("t", "u1"),
            prompt: "hello".into(),
            reply_ctx: ReplyCtx::default(),
            schedule: ScheduleKind::Once {
                fire_at_ms: 1_735_689_600_000,
            },
            created_at_ms: 0,
            last_fired_ms: None,
        };
        let s = entry.display_schedule();
        assert!(s.contains("once at"));
        assert!(s.contains("2025-01-01"));
    }

    #[test]
    fn display_schedule_recurring() {
        let entry = ScheduleEntry {
            id: "test".into(),
            key: SessionKey::new("t", "u1"),
            prompt: "hello".into(),
            reply_ctx: ReplyCtx::default(),
            schedule: ScheduleKind::Recurring {
                interval_ms: 3_600_000,
                next_fire_ms: 1_735_689_600_000,
            },
            created_at_ms: 0,
            last_fired_ms: None,
        };
        let s = entry.display_schedule();
        assert!(s.contains("every 1 hour"));
        assert!(s.contains("next:"));
    }

    #[test]
    fn parse_duration_ms_all_units() {
        assert_eq!(parse_duration_ms("10s").unwrap(), 10_000);
        assert_eq!(parse_duration_ms("5m").unwrap(), 300_000);
        assert_eq!(parse_duration_ms("2h").unwrap(), 7_200_000);
        assert_eq!(parse_duration_ms("1d").unwrap(), 86_400_000);
    }

    #[test]
    fn parse_duration_ms_errors() {
        assert!(parse_duration_ms("").is_err());
        assert!(parse_duration_ms("0m").is_err());
        assert!(parse_duration_ms("abc").is_err());
        assert!(parse_duration_ms("10x").is_err());
    }

    #[test]
    fn parse_hh_mm_valid() {
        assert_eq!(parse_hh_mm("09:00").unwrap(), (9, 0));
        assert_eq!(parse_hh_mm("23:59").unwrap(), (23, 59));
        assert_eq!(parse_hh_mm("00:00").unwrap(), (0, 0));
    }

    #[test]
    fn parse_hh_mm_invalid() {
        assert!(parse_hh_mm("24:00").is_err());
        assert!(parse_hh_mm("12:60").is_err());
        assert!(parse_hh_mm("1200").is_err());
        assert!(parse_hh_mm("ab:cd").is_err());
    }

    #[test]
    fn serde_round_trip_once() {
        let kind = ScheduleKind::Once {
            fire_at_ms: 1_000_000,
        };
        let json = serde_json::to_string(&kind).unwrap();
        let back: ScheduleKind = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            back,
            ScheduleKind::Once {
                fire_at_ms: 1_000_000
            }
        ));
    }

    #[test]
    fn serde_round_trip_recurring() {
        let kind = ScheduleKind::Recurring {
            interval_ms: 60_000,
            next_fire_ms: 2_000_000,
        };
        let json = serde_json::to_string(&kind).unwrap();
        let back: ScheduleKind = serde_json::from_str(&json).unwrap();
        match back {
            ScheduleKind::Recurring {
                interval_ms,
                next_fire_ms,
            } => {
                assert_eq!(interval_ms, 60_000);
                assert_eq!(next_fire_ms, 2_000_000);
            }
            _ => panic!("expected Recurring"),
        }
    }

    #[test]
    fn serde_round_trip_entry() {
        let entry = ScheduleEntry {
            id: "abc-123".into(),
            key: SessionKey::new("line", "U1234"),
            prompt: "check status".into(),
            reply_ctx: ReplyCtx::default(),
            schedule: ScheduleKind::Recurring {
                interval_ms: 3_600_000,
                next_fire_ms: 1_700_000_000_000,
            },
            created_at_ms: 1_699_000_000_000,
            last_fired_ms: None,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ScheduleEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "abc-123");
        assert_eq!(back.key, SessionKey::new("line", "U1234"));
        assert_eq!(back.prompt, "check status");
    }

    // ── Scheduler CRUD tests ────────────────────────────────────────────

    fn make_entry(id: &str, key: &SessionKey, prompt: &str) -> ScheduleEntry {
        ScheduleEntry {
            id: id.into(),
            key: key.clone(),
            prompt: prompt.into(),
            reply_ctx: ReplyCtx::default(),
            schedule: ScheduleKind::Recurring {
                interval_ms: 3_600_000,
                next_fire_ms: now_ms() + 3_600_000,
            },
            created_at_ms: now_ms(),
            last_fired_ms: None,
        }
    }

    fn make_test_db() -> Arc<Mutex<StateDb>> {
        Arc::new(Mutex::new(StateDb::in_memory()))
    }

    #[tokio::test]
    async fn scheduler_add_and_list() {
        let db = make_test_db();
        let sched = Scheduler { db };

        let key = SessionKey::new("t", "u1");
        sched.add(make_entry("s1", &key, "hello")).await.unwrap();
        sched.add(make_entry("s2", &key, "world")).await.unwrap();

        let list = sched.list(&key).await;
        assert_eq!(list.len(), 2);

        let all = sched.list_all().await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn scheduler_list_filters_by_key() {
        let db = make_test_db();
        let sched = Scheduler { db };

        let k1 = SessionKey::new("t", "u1");
        let k2 = SessionKey::new("t", "u2");
        sched.add(make_entry("s1", &k1, "one")).await.unwrap();
        sched.add(make_entry("s2", &k2, "two")).await.unwrap();

        assert_eq!(sched.list(&k1).await.len(), 1);
        assert_eq!(sched.list(&k2).await.len(), 1);
        assert_eq!(sched.list_all().await.len(), 2);
    }

    #[tokio::test]
    async fn scheduler_remove() {
        let db = make_test_db();
        let sched = Scheduler { db };

        let key = SessionKey::new("t", "u1");
        sched.add(make_entry("s1", &key, "hello")).await.unwrap();
        sched.add(make_entry("s2", &key, "world")).await.unwrap();

        let removed = sched.remove(&key, "s1").await.unwrap();
        assert!(removed);
        assert_eq!(sched.list(&key).await.len(), 1);
        assert_eq!(sched.list(&key).await[0].id, "s2");

        let not_found = sched.remove(&key, "nonexistent").await.unwrap();
        assert!(!not_found);
    }

    #[tokio::test]
    async fn scheduler_remove_wrong_key() {
        let db = make_test_db();
        let sched = Scheduler { db };

        let k1 = SessionKey::new("t", "u1");
        let k2 = SessionKey::new("t", "u2");
        sched.add(make_entry("s1", &k1, "hello")).await.unwrap();

        let removed = sched.remove(&k2, "s1").await.unwrap();
        assert!(!removed);
        assert_eq!(sched.list_all().await.len(), 1);
    }
}
