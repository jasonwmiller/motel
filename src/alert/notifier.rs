use std::process::Command;

/// Target for alert notifications.
#[derive(Debug, Clone)]
pub enum NotificationTarget {
    Stderr,
    Webhook { url: String },
    ShellCommand { cmd: String },
}

/// A notification payload sent when an alert fires.
#[derive(Debug, Clone)]
pub struct AlertNotification {
    pub rule: String,
    pub message: String,
    pub timestamp: String,
}

/// Escape a string for safe inclusion in a shell command.
///
/// Wraps the value in single quotes and escapes any internal single quotes
/// using the `'\''` idiom (end quote, escaped quote, start quote).
fn shell_escape(s: &str) -> String {
    let escaped = s.replace('\'', "'\\''");
    format!("'{}'", escaped)
}

impl NotificationTarget {
    pub async fn send(&self, notification: &AlertNotification) {
        match self {
            Self::Stderr => {
                eprintln!("[ALERT] {} — {}", notification.rule, notification.message);
            }
            Self::Webhook { url } => {
                let client = reqwest::Client::new();
                let body = serde_json::json!({
                    "rule": notification.rule,
                    "message": notification.message,
                    "timestamp": notification.timestamp,
                });
                match client.post(url).json(&body).send().await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("alert webhook failed: {e}");
                    }
                }
            }
            Self::ShellCommand { cmd } => {
                let escaped_message = shell_escape(&notification.message);
                let escaped_rule = shell_escape(&notification.rule);
                let full_cmd = cmd
                    .replace("{message}", &escaped_message)
                    .replace("{rule}", &escaped_rule);
                match Command::new("sh").args(["-c", &full_cmd]).spawn() {
                    Ok(mut child) => {
                        let _ = child.wait();
                    }
                    Err(e) => {
                        tracing::warn!("alert command failed: {e}");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shell_escape_plain_string() {
        assert_eq!(shell_escape("hello world"), "'hello world'");
    }

    #[test]
    fn test_shell_escape_single_quotes() {
        assert_eq!(shell_escape("it's here"), "'it'\\''s here'");
    }

    #[test]
    fn test_shell_escape_injection_attempt() {
        let malicious = "$(rm -rf /)";
        let escaped = shell_escape(malicious);
        assert_eq!(escaped, "'$(rm -rf /)'");
        // The value is safely wrapped in single quotes, preventing expansion
    }

    #[test]
    fn test_shell_escape_backticks_and_semicolons() {
        let malicious = "`whoami`; echo pwned";
        let escaped = shell_escape(malicious);
        assert_eq!(escaped, "'`whoami`; echo pwned'");
    }
}
