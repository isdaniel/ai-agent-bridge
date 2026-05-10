//! Linux systemd-user service install/uninstall/start/stop helpers.
//!
//! Writes a unit file under `~/.config/systemd/user/` and shells out to
//! `systemctl --user`. Operators who want a different layout (system unit,
//! `pm2`, supervisord, etc.) should disable this and bring their own.

use std::path::PathBuf;
use std::process::{Command, Output};

use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceStatus {
    Running,
    Stopped,
    NotInstalled,
}

pub trait ServiceManager: Send + Sync {
    fn install(&self, exe: &std::path::Path, args: &[String]) -> Result<()>;
    fn uninstall(&self) -> Result<()>;
    fn start(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;
    fn status(&self) -> Result<ServiceStatus>;
}

pub fn platform_service(name: &str) -> Box<dyn ServiceManager> {
    Box::new(SystemdUser::new(name))
}

fn run(cmd: &mut Command) -> Result<Output> {
    let out = cmd
        .output()
        .with_context(|| format!("spawn {:?}", cmd.get_program()))?;
    if !out.status.success() {
        anyhow::bail!(
            "{:?} {:?} failed: {}",
            cmd.get_program(),
            cmd.get_args().collect::<Vec<_>>(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(out)
}

pub struct SystemdUser {
    name: String,
    unit_path: PathBuf,
}

impl SystemdUser {
    pub fn new(name: &str) -> Self {
        let unit_path = dirs::config_dir()
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".config")
            })
            .join("systemd")
            .join("user")
            .join(format!("{name}.service"));
        Self {
            name: name.to_string(),
            unit_path,
        }
    }

    fn unit_name(&self) -> String {
        format!("{}.service", self.name)
    }
}

impl ServiceManager for SystemdUser {
    fn install(&self, exe: &std::path::Path, args: &[String]) -> Result<()> {
        if let Some(parent) = self.unit_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let exec_start = format!(
            "{} {}",
            shell_escape(&exe.to_string_lossy()),
            args.iter()
                .map(|a| shell_escape(a))
                .collect::<Vec<_>>()
                .join(" ")
        );
        let unit = format!(
            "[Unit]\n\
             Description=ai-agent-bridge ({name})\n\
             After=network.target\n\
             \n\
             [Service]\n\
             Type=simple\n\
             ExecStart={exec_start}\n\
             Restart=on-failure\n\
             RestartSec=5s\n\
             \n\
             [Install]\n\
             WantedBy=default.target\n",
            name = self.name,
            exec_start = exec_start,
        );
        std::fs::write(&self.unit_path, unit)?;
        run(Command::new("systemctl").args(["--user", "daemon-reload"]))?;
        run(Command::new("systemctl").args(["--user", "enable", &self.unit_name()]))?;
        Ok(())
    }
    fn uninstall(&self) -> Result<()> {
        let _ = run(Command::new("systemctl").args(["--user", "disable", &self.unit_name()]));
        let _ = run(Command::new("systemctl").args(["--user", "stop", &self.unit_name()]));
        if self.unit_path.exists() {
            std::fs::remove_file(&self.unit_path)?;
        }
        run(Command::new("systemctl").args(["--user", "daemon-reload"]))?;
        Ok(())
    }
    fn start(&self) -> Result<()> {
        run(Command::new("systemctl").args(["--user", "start", &self.unit_name()]))?;
        Ok(())
    }
    fn stop(&self) -> Result<()> {
        run(Command::new("systemctl").args(["--user", "stop", &self.unit_name()]))?;
        Ok(())
    }
    fn status(&self) -> Result<ServiceStatus> {
        if !self.unit_path.exists() {
            return Ok(ServiceStatus::NotInstalled);
        }
        let out = Command::new("systemctl")
            .args(["--user", "is-active", &self.unit_name()])
            .output()
            .context("systemctl is-active")?;
        let txt = String::from_utf8_lossy(&out.stdout).trim().to_string();
        Ok(match txt.as_str() {
            "active" => ServiceStatus::Running,
            _ => ServiceStatus::Stopped,
        })
    }
}

fn shell_escape(s: &str) -> String {
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || "-_./=:".contains(c))
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}
