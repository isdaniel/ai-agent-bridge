//! Daemon plumbing: single-instance lock, rotating log writer, and OS-native
//! service install/uninstall/start/stop helpers.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use fd_lock::RwLock;
use tracing_appender::non_blocking::WorkerGuard;

pub mod service;

pub use service::{platform_service, ServiceManager, ServiceStatus};

/// RAII guard for a single-instance file lock. Hold for the lifetime of the
/// process; drop to release. Internally leaks the underlying `RwLock` so the
/// fd-lock guard's lifetime can be `'static`.
pub struct LockGuard {
    _guard: fd_lock::RwLockWriteGuard<'static, std::fs::File>,
    _path: PathBuf,
}

impl LockGuard {
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p).ok();
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open lock {}", path.display()))?;
        // Leak the RwLock so its &mut reference can be 'static, satisfying the
        // guard lifetime requirement. Acceptable: we hold one per process.
        let lock_ref: &'static mut RwLock<std::fs::File> = Box::leak(Box::new(RwLock::new(file)));
        match lock_ref.try_write() {
            Ok(guard) => Ok(Self {
                _guard: guard,
                _path: path,
            }),
            Err(_) => anyhow::bail!(
                "another instance holds the daemon lock at {}",
                path.display()
            ),
        }
    }
}

/// Set up a daily-rotating log file under `dir/aab.log`. Returns a worker
/// guard that must be kept alive for the lifetime of the process.
pub fn init_rotating_logs(dir: impl AsRef<Path>) -> Result<WorkerGuard> {
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir)?;
    let file_appender = tracing_appender::rolling::daily(dir, "aab.log");
    let (nb, guard) = tracing_appender::non_blocking(file_appender);
    let subscriber = tracing_subscriber_init(nb);
    tracing::subscriber::set_global_default(subscriber).ok();
    Ok(guard)
}

fn tracing_subscriber_init<W>(writer: W) -> impl tracing::Subscriber + Send + Sync
where
    W: for<'a> tracing_subscriber::fmt::MakeWriter<'a> + Send + Sync + 'static,
{
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(writer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_lock_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aab.lock");
        let _g1 = LockGuard::acquire(&path).expect("first acquire");
        let g2 = LockGuard::acquire(&path);
        assert!(g2.is_err(), "second acquire should fail");
    }
}
