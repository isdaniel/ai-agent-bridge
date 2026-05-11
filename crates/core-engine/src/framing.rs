//! Newline-delimited JSON framing helpers shared by stream-json (Claude Code)
//! and ACP transports. Also hosts shared agent-process lifecycle utilities.

use anyhow::Result;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::{info, warn};

/// 8 MiB default — large enough to carry inline base64 images.
pub const DEFAULT_MAX_LINE: usize = 8 * 1024 * 1024;

/// Default capacity for per-session event channels.
pub const EVENTS_CAP: usize = 64;

/// Default grace period before force-killing an agent subprocess.
pub const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(120);

/// Spawn a reader task that decodes one JSON value per line and forwards
/// successfully parsed values into `tx`. Parse errors are logged and skipped
/// (do not tear down the stream).
pub fn spawn_ndjson_reader<R, T>(
    reader: R,
    max_line: usize,
    tx: mpsc::Sender<T>,
) -> tokio::task::JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    T: DeserializeOwned + Send + 'static,
{
    let codec = LinesCodec::new_with_max_length(max_line);
    let mut framed = FramedRead::new(reader, codec);
    tokio::spawn(async move {
        while let Some(line_res) = framed.next().await {
            match line_res {
                Ok(line) if line.trim().is_empty() => continue,
                Ok(line) => match serde_json::from_str::<T>(&line) {
                    Ok(v) => {
                        if tx.send(v).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => warn!(error = %e, line = %truncate(&line, 200), "parse fail"),
                },
                Err(e) => {
                    warn!(error = %e, "ndjson read error");
                    break;
                }
            }
        }
    })
}

/// Serialize `value` to a single NDJSON line and write it.
pub async fn write_ndjson<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let mut buf = serde_json::to_vec(value)?;
    buf.push(b'\n');
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

/// Wait for `child` to exit within `grace`, then force-kill.
/// Shared by both `agent-claude-code` and `agent-acp` close paths.
pub async fn shutdown_child(child: &mut tokio::process::Child, grace: std::time::Duration) {
    match tokio::time::timeout(grace, child.wait()).await {
        Ok(Ok(status)) if status.success() => info!("agent exited cleanly"),
        Ok(Ok(status)) => warn!(code = ?status.code(), "agent exited with error"),
        _ => {
            warn!("agent shutdown grace exceeded; killing");
            let _ = child.start_kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tokio::io::duplex;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Msg {
        a: i32,
    }

    #[tokio::test]
    async fn ndjson_round_trip() {
        let (mut w, r) = duplex(1024);
        let (tx, mut rx) = mpsc::channel(8);
        let handle = spawn_ndjson_reader::<_, Msg>(r, 1024, tx);

        write_ndjson(&mut w, &Msg { a: 1 }).await.unwrap();
        write_ndjson(&mut w, &Msg { a: 2 }).await.unwrap();
        drop(w);

        assert_eq!(rx.recv().await, Some(Msg { a: 1 }));
        assert_eq!(rx.recv().await, Some(Msg { a: 2 }));
        assert!(rx.recv().await.is_none());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn ndjson_skips_bad_lines() {
        let (mut w, r) = duplex(1024);
        let (tx, mut rx) = mpsc::channel(8);
        let _handle = spawn_ndjson_reader::<_, Msg>(r, 1024, tx);

        w.write_all(b"not json\n").await.unwrap();
        write_ndjson(&mut w, &Msg { a: 7 }).await.unwrap();
        drop(w);

        assert_eq!(rx.recv().await, Some(Msg { a: 7 }));
    }
}
