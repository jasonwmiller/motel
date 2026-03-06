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
                let full_cmd = cmd
                    .replace("{message}", &notification.message)
                    .replace("{rule}", &notification.rule);
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
