//! Slash command parser + registry with template expansion.
//!
//! Names are normalized: `-` and `_` are equivalent and the lookup is
//! case-insensitive (so `/MyCmd`, `/my-cmd`, and `/my_cmd` all match).
//!
//! Templates support:
//!   * `{{1}} {{2}} ...` — positional arguments (1-indexed)
//!   * `{{2*}}`          — argument 2 onward, joined by space
//!   * `{{args}}`        — all arguments, joined by space
//!
//! Built-ins are registered first by the engine; subsequent attempts to
//! register a name with `Source::Config` or `Source::Agent` are rejected.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Builtin,
    Config,
    Agent,
}

#[derive(Clone, Debug)]
pub struct CommandSpec {
    pub name: String,
    pub source: Source,
    pub template: String,
    pub description: String,
}

#[derive(Debug, Error)]
pub enum RegisterError {
    #[error("command `{name}` already registered (source={existing:?})")]
    Conflict { name: String, existing: Source },
}

#[derive(Default)]
pub struct CommandRegistry {
    by_name: HashMap<String, CommandSpec>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn normalize(name: &str) -> String {
        name.trim_start_matches('/')
            .replace('-', "_")
            .to_lowercase()
    }

    pub fn register(&mut self, spec: CommandSpec) -> Result<(), RegisterError> {
        let key = Self::normalize(&spec.name);
        if let Some(existing) = self.by_name.get(&key) {
            // Builtin always wins; otherwise reject.
            if existing.source == Source::Builtin {
                return Err(RegisterError::Conflict {
                    name: key,
                    existing: existing.source,
                });
            }
            if spec.source == Source::Builtin {
                self.by_name.insert(key, spec);
                return Ok(());
            }
            return Err(RegisterError::Conflict {
                name: key,
                existing: existing.source,
            });
        }
        self.by_name.insert(key, spec);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&CommandSpec> {
        self.by_name.get(&Self::normalize(name))
    }

    pub fn list(&self) -> impl Iterator<Item = &CommandSpec> {
        self.by_name.values()
    }

    /// Expand `template` against `args`. Missing positional args become "".
    pub fn expand(&self, name: &str, args: &[&str]) -> Option<String> {
        let spec = self.get(name)?;
        Some(expand_template(&spec.template, args))
    }
}

/// Parse a chat line. Returns `Some((name, args))` if it begins with `/`.
/// Quoted args (`"two words"`) are preserved as one arg.
pub fn parse_command_line(line: &str) -> Option<(String, Vec<String>)> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix('/')?;
    let mut tokens = tokenize(rest);
    if tokens.is_empty() {
        return None;
    }
    let name = tokens.remove(0);
    Some((name, tokens))
}

fn tokenize(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut chars = s.chars().peekable();
    let mut in_quotes = false;
    while let Some(c) = chars.next() {
        match c {
            '"' => in_quotes = !in_quotes,
            '\\' if in_quotes => {
                if let Some(esc) = chars.next() {
                    buf.push(esc);
                }
            }
            c if c.is_whitespace() && !in_quotes => {
                if !buf.is_empty() {
                    out.push(std::mem::take(&mut buf));
                }
            }
            c => buf.push(c),
        }
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}

fn expand_template(template: &str, args: &[&str]) -> String {
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            if let Some(end) = find_close(bytes, i + 2) {
                let inner = &template[i + 2..end];
                if let Some(replaced) = expand_token(inner, args) {
                    out.push_str(&replaced);
                    i = end + 2;
                    continue;
                }
            }
        }
        // Walk one full UTF-8 codepoint, not one byte.
        let ch_end = next_char_boundary(template, i);
        out.push_str(&template[i..ch_end]);
        i = ch_end;
    }
    out
}

fn next_char_boundary(s: &str, i: usize) -> usize {
    let mut j = i + 1;
    while j < s.len() && !s.is_char_boundary(j) {
        j += 1;
    }
    j
}

fn find_close(bytes: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < bytes.len() {
        if bytes[i] == b'}' && bytes[i + 1] == b'}' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn expand_token(tok: &str, args: &[&str]) -> Option<String> {
    let tok = tok.trim();
    if tok == "args" {
        return Some(args.join(" "));
    }
    if let Some(stripped) = tok.strip_suffix('*') {
        let n: usize = stripped.parse().ok()?;
        if n == 0 {
            return None;
        }
        let from = n - 1;
        if from >= args.len() {
            return Some(String::new());
        }
        return Some(args[from..].join(" "));
    }
    let n: usize = tok.parse().ok()?;
    if n == 0 {
        return None;
    }
    Some(args.get(n - 1).copied().unwrap_or("").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, src: Source, template: &str) -> CommandSpec {
        CommandSpec {
            name: name.into(),
            source: src,
            template: template.into(),
            description: "".into(),
        }
    }

    #[test]
    fn normalize_case_and_dash() {
        assert_eq!(CommandRegistry::normalize("/MyCmd"), "mycmd");
        assert_eq!(CommandRegistry::normalize("/my-cmd"), "my_cmd");
        assert_eq!(CommandRegistry::normalize("my_cmd"), "my_cmd");
    }

    #[test]
    fn builtin_collision_rejected() {
        let mut reg = CommandRegistry::new();
        reg.register(spec("reset", Source::Builtin, "reset"))
            .unwrap();
        let err = reg
            .register(spec("reset", Source::Config, "do other"))
            .unwrap_err();
        assert!(matches!(err, RegisterError::Conflict { .. }));
    }

    #[test]
    fn template_positional_and_star() {
        let mut reg = CommandRegistry::new();
        reg.register(spec("repeat", Source::Builtin, "{{1}}: {{2*}}"))
            .unwrap();
        assert_eq!(
            reg.expand("repeat", &["TODO", "fix", "the", "bug"]),
            Some("TODO: fix the bug".into())
        );
    }

    #[test]
    fn template_args_alias() {
        let mut reg = CommandRegistry::new();
        reg.register(spec("echo", Source::Builtin, "→ {{args}}"))
            .unwrap();
        assert_eq!(reg.expand("echo", &["a", "b"]), Some("→ a b".into()));
    }

    #[test]
    fn parse_handles_quotes() {
        let (name, args) = parse_command_line("/say \"hello world\" two").unwrap();
        assert_eq!(name, "say");
        assert_eq!(args, vec!["hello world".to_string(), "two".to_string()]);
    }

    #[test]
    fn parse_returns_none_without_slash() {
        assert!(parse_command_line("plain text").is_none());
    }
}
