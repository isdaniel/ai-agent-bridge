---
name: schedule
description: Schedule timed or recurring actions using natural language. Use when the user wants to set a reminder, schedule a task, create a recurring action, or manage their scheduled items. Triggers on phrases like "remind me", "schedule", "排程", "提醒我", "每天", "at 9am", "in 30 minutes", "幫我設定", or any time-based future action request.
---

# Schedule Actions

排程管理：建立、查看、刪除定時或週期性動作。

**IMPORTANT:** This skill is the ONLY correct way to schedule actions in this project. Do NOT use `CronCreate` / `CronDelete` / `CronList` — those write to Claude Code's internal store and the chat bridge cannot see them, so the user will not find them in `/schedule-list` and cannot manage them.

## How it works

Include a hidden marker in your response. The bridge intercepts it and creates/queries/deletes schedule entries internally. The user never sees the marker — only your natural language confirmation.

## Creating a schedule

Include this marker **anywhere** in your response (it will be stripped):

```
<!--aab:schedule {"when":"<time_spec>","prompt":"<action_prompt>"}-->
```

### Time spec formats (all times UTC)

| Format | Example | Meaning |
|--------|---------|---------|
| `in <N><unit>` | `in 30m`, `in 2h`, `in 1d` | One-shot, relative |
| `at HH:MM` | `at 09:00`, `at 14:30` | One-shot, next occurrence |
| `at YYYY-MM-DD HH:MM` | `at 2025-06-01 09:00` | One-shot, absolute |
| `every <N><unit>` | `every 1h`, `every 30m` | Recurring interval |
| `every day HH:MM` | `every day 09:00` | Daily at fixed time |

Units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

### Natural language → time spec conversion

Convert the user's language to the closest format:
- "30分鐘後" / "30分後" / "in 30 minutes" → `in 30m`
- "明天早上九點" / "tomorrow 9am" → `at 09:00` (adjust for user's intent; all times UTC)
- "每天早上九點" / "every day at 9am" → `every day 09:00`
- "每小時" / "every hour" → `every 1h`
- "一小時後" / "in an hour" → `in 1h`
- "下午三點" / "at 3pm" → `at 15:00`

### The prompt field

The `prompt` is what gets sent to the AI agent when the schedule fires — as if the user typed it. Use it to describe the full action:

- For voice calls: `"打電話給 +886975953133，播放：請記得拿出匯票"`
- For reminders: `"提醒：下午三點有會議"`
- For recurring checks: `"請檢查伺服器狀態並回報"`

### Examples

User: "幫我明天早上九點打電話提醒我拿匯票，我的電話是 +886975953133"
Response:
```
好的，我已幫您排程明天早上九點（UTC）撥打電話提醒您拿匯票。
<!--aab:schedule {"when":"at 09:00","prompt":"打電話給 +886975953133，播放訊息：提醒您今天要記得拿出匯票。"}-->
```

User: "every 2 hours, check server status"
Response:
```
Done! I've scheduled a recurring check every 2 hours.
<!--aab:schedule {"when":"every 2h","prompt":"Please check the server status and report any issues."}-->
```

User: "remind me in 30 minutes to review the PR"
Response:
```
Got it — I'll remind you in 30 minutes.
<!--aab:schedule {"when":"in 30m","prompt":"Reminder: time to review the PR."}-->
```

## Listing schedules

Include this marker:

```
<!--aab:schedule-list-->
```

The bridge will append a formatted list of all active schedules for this user.

Example:
User: "我目前有哪些排程？" / "show my schedules"
Response:
```
讓我查看您的排程：
<!--aab:schedule-list-->
```

## Deleting a schedule

Include this marker with the schedule ID (or prefix):

```
<!--aab:schedule-delete {"id":"<id_prefix>"}-->
```

Example:
User: "取消 deadbeef 排程" / "delete schedule deadbeef"
Response:
```
已刪除排程。
<!--aab:schedule-delete {"id":"deadbeef"}-->
```

## Combining with other skills

The prompt in a schedule can trigger any skill when it fires. For example:
- Schedule + voice-call: `{"prompt":"打電話給 +886... 播放：開會提醒"}` — when fired, the agent receives this as a message and uses the voice-call skill
- Schedule + web-search: `{"prompt":"search for latest Bitcoin price and report"}` — agent uses web-search skill

## Important notes

- All times are UTC. If the user specifies a timezone, convert to UTC before setting the schedule.
- The marker MUST be valid JSON. Escape quotes in the prompt if needed.
- Always confirm the schedule to the user in natural language — the bridge will also append a confirmation with the schedule ID.
- For listing/deleting, the bridge appends the results — you don't need to fabricate a list.
