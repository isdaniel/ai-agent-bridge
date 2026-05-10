//! Slack `files.uploadV2` three-step upload.
//!
//! 1. POST `files.getUploadURLExternal` → returns `{upload_url, file_id}`
//! 2. PUT raw bytes to `upload_url`
//! 3. POST `files.completeUploadExternal` with `{files:[{id,title}], channel_id}`

use anyhow::{anyhow, Context};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct GetUrlResp {
    ok: bool,
    error: Option<String>,
    upload_url: Option<String>,
    file_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CompleteResp {
    ok: bool,
    error: Option<String>,
}

pub async fn upload_file(
    http: &reqwest::Client,
    bot_token: &str,
    channel: &str,
    bytes: bytes::Bytes,
    filename: &str,
) -> anyhow::Result<String> {
    let len = bytes.len();
    let resp: GetUrlResp = http
        .get("https://slack.com/api/files.getUploadURLExternal")
        .bearer_auth(bot_token)
        .query(&[("filename", filename), ("length", &len.to_string())])
        .send()
        .await
        .context("getUploadURLExternal request")?
        .json()
        .await
        .context("decode getUploadURLExternal")?;

    if !resp.ok {
        return Err(anyhow!(
            "getUploadURLExternal failed: {}",
            resp.error.unwrap_or_default()
        ));
    }
    let upload_url = resp
        .upload_url
        .ok_or_else(|| anyhow!("missing upload_url"))?;
    let file_id = resp.file_id.ok_or_else(|| anyhow!("missing file_id"))?;

    let put = http.post(&upload_url).body(bytes).send().await?;
    if !put.status().is_success() {
        return Err(anyhow!("PUT to upload_url failed: HTTP {}", put.status()));
    }

    let complete: CompleteResp = http
        .post("https://slack.com/api/files.completeUploadExternal")
        .bearer_auth(bot_token)
        .json(&serde_json::json!({
            "files": [{ "id": file_id, "title": filename }],
            "channel_id": channel,
        }))
        .send()
        .await
        .context("completeUploadExternal request")?
        .json()
        .await
        .context("decode completeUploadExternal")?;

    if !complete.ok {
        return Err(anyhow!(
            "completeUploadExternal failed: {}",
            complete.error.unwrap_or_default()
        ));
    }
    Ok(file_id)
}
