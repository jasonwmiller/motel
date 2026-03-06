use std::time::Duration;

/// A parsed alert rule from the CLI --alert flag.
#[derive(Debug, Clone)]
pub struct AlertRule {
    pub condition: AlertCondition,
    pub raw: String,
}

#[derive(Debug, Clone)]
pub enum AlertCondition {
    /// Fires when any individual span duration exceeds threshold
    SpanDuration { threshold: Duration },
    /// Fires when error status spans per window exceed count
    ErrorRate { max_count: u64, window: Duration },
    /// Fires when a log body contains the given substring
    LogBodyContains { pattern: String },
    /// Fires when a log severity >= the given level
    LogSeverityAtLeast { min_severity: i32 },
    /// Fires when a metric value exceeds a threshold
    MetricThreshold {
        metric_name: String,
        op: CmpOp,
        threshold: f64,
    },
}

#[derive(Debug, Clone)]
pub enum CmpOp {
    Gt,
    Lt,
    Gte,
    Lte,
    Eq,
}

impl CmpOp {
    pub fn eval(&self, value: f64, threshold: f64) -> bool {
        match self {
            CmpOp::Gt => value > threshold,
            CmpOp::Lt => value < threshold,
            CmpOp::Gte => value >= threshold,
            CmpOp::Lte => value <= threshold,
            CmpOp::Eq => (value - threshold).abs() < f64::EPSILON,
        }
    }
}

impl AlertRule {
    /// Parse a rule string into an AlertRule.
    ///
    /// Supported formats:
    /// - `"span_duration > 5s"` or `"span_duration > 500ms"`
    /// - `"error_rate > 10/min"` or `"error_rate > 10/1m"`
    /// - `"log_body contains 'panic'"` or `"log_body contains 'some text'"`
    /// - `"log_severity >= ERROR"`
    /// - `"metric cpu.usage > 90.0"`
    pub fn parse(s: &str) -> Result<AlertRule, String> {
        let s = s.trim();

        if s.starts_with("span_duration") {
            return Self::parse_span_duration(s);
        }
        if s.starts_with("error_rate") {
            return Self::parse_error_rate(s);
        }
        if s.starts_with("log_body") {
            return Self::parse_log_body(s);
        }
        if s.starts_with("log_severity") {
            return Self::parse_log_severity(s);
        }
        if s.starts_with("metric") {
            return Self::parse_metric_threshold(s);
        }

        Err(format!(
            "unknown alert rule type: {s}. Expected one of: span_duration, error_rate, log_body, log_severity, metric"
        ))
    }

    fn parse_span_duration(s: &str) -> Result<AlertRule, String> {
        // "span_duration > 5s" or "span_duration > 500ms"
        let rest = s
            .strip_prefix("span_duration")
            .ok_or("expected 'span_duration'")?
            .trim();
        let rest = rest
            .strip_prefix('>')
            .ok_or("expected '>' after span_duration")?
            .trim();
        let threshold = parse_duration_str(rest)?;
        Ok(AlertRule {
            condition: AlertCondition::SpanDuration { threshold },
            raw: s.to_string(),
        })
    }

    fn parse_error_rate(s: &str) -> Result<AlertRule, String> {
        // "error_rate > 10/min" or "error_rate > 10/1m"
        let rest = s
            .strip_prefix("error_rate")
            .ok_or("expected 'error_rate'")?
            .trim();
        let rest = rest
            .strip_prefix('>')
            .ok_or("expected '>' after error_rate")?
            .trim();
        let parts: Vec<&str> = rest.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(format!(
                "expected 'COUNT/WINDOW' in error_rate rule, got: {rest}"
            ));
        }
        let max_count: u64 = parts[0]
            .trim()
            .parse()
            .map_err(|_| format!("invalid count: {}", parts[0]))?;
        let window = parse_window_str(parts[1].trim())?;
        Ok(AlertRule {
            condition: AlertCondition::ErrorRate { max_count, window },
            raw: s.to_string(),
        })
    }

    fn parse_log_body(s: &str) -> Result<AlertRule, String> {
        // "log_body contains 'panic'"
        let rest = s
            .strip_prefix("log_body")
            .ok_or("expected 'log_body'")?
            .trim();
        let rest = rest
            .strip_prefix("contains")
            .ok_or("expected 'contains' after log_body")?
            .trim();
        let pattern = parse_quoted_string(rest)?;
        Ok(AlertRule {
            condition: AlertCondition::LogBodyContains { pattern },
            raw: s.to_string(),
        })
    }

    fn parse_log_severity(s: &str) -> Result<AlertRule, String> {
        // "log_severity >= ERROR"
        let rest = s
            .strip_prefix("log_severity")
            .ok_or("expected 'log_severity'")?
            .trim();
        let rest = rest
            .strip_prefix(">=")
            .ok_or("expected '>=' after log_severity")?
            .trim();
        let min_severity = severity_text_to_number(rest)?;
        Ok(AlertRule {
            condition: AlertCondition::LogSeverityAtLeast { min_severity },
            raw: s.to_string(),
        })
    }

    fn parse_metric_threshold(s: &str) -> Result<AlertRule, String> {
        // "metric cpu.usage > 90.0"
        let rest = s.strip_prefix("metric").ok_or("expected 'metric'")?.trim();
        // Find the operator
        let (metric_name, op, threshold) = parse_metric_expr(rest)?;
        Ok(AlertRule {
            condition: AlertCondition::MetricThreshold {
                metric_name,
                op,
                threshold,
            },
            raw: s.to_string(),
        })
    }
}

fn parse_metric_expr(s: &str) -> Result<(String, CmpOp, f64), String> {
    // Try each operator, longest first
    for (op_str, op) in &[
        (">=", CmpOp::Gte),
        ("<=", CmpOp::Lte),
        (">", CmpOp::Gt),
        ("<", CmpOp::Lt),
        ("=", CmpOp::Eq),
    ] {
        if let Some(pos) = s.find(op_str) {
            let name = s[..pos].trim().to_string();
            let val_str = s[pos + op_str.len()..].trim();
            let threshold: f64 = val_str
                .parse()
                .map_err(|_| format!("invalid threshold: {val_str}"))?;
            if name.is_empty() {
                return Err("metric name cannot be empty".to_string());
            }
            return Ok((name, op.clone(), threshold));
        }
    }
    Err(format!("no comparison operator found in metric rule: {s}"))
}

/// Parse a duration string like "5s", "500ms", "1m", "2h", "1d"
fn parse_duration_str(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if let Some(num_str) = s.strip_suffix("ms") {
        let num: u64 = num_str
            .parse()
            .map_err(|_| format!("invalid duration: {s}"))?;
        return Ok(Duration::from_millis(num));
    }
    if let Some(num_str) = s.strip_suffix('s') {
        let num: u64 = num_str
            .parse()
            .map_err(|_| format!("invalid duration: {s}"))?;
        return Ok(Duration::from_secs(num));
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let num: u64 = num_str
            .parse()
            .map_err(|_| format!("invalid duration: {s}"))?;
        return Ok(Duration::from_secs(num * 60));
    }
    if let Some(num_str) = s.strip_suffix('h') {
        let num: u64 = num_str
            .parse()
            .map_err(|_| format!("invalid duration: {s}"))?;
        return Ok(Duration::from_secs(num * 3600));
    }
    if let Some(num_str) = s.strip_suffix('d') {
        let num: u64 = num_str
            .parse()
            .map_err(|_| format!("invalid duration: {s}"))?;
        return Ok(Duration::from_secs(num * 86400));
    }
    Err(format!(
        "invalid duration: {s}. Expected format like 5s, 500ms, 1m, 2h, 1d"
    ))
}

/// Parse a window string like "min", "1m", "5m", "1h"
fn parse_window_str(s: &str) -> Result<Duration, String> {
    match s {
        "min" | "minute" => Ok(Duration::from_secs(60)),
        "hr" | "hour" => Ok(Duration::from_secs(3600)),
        _ => parse_duration_str(s),
    }
}

/// Parse a single-quoted string: 'some text' -> some text
fn parse_quoted_string(s: &str) -> Result<String, String> {
    let s = s.trim();
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        Ok(s[1..s.len() - 1].to_string())
    } else {
        Err(format!("expected single-quoted string, got: {s}"))
    }
}

/// Convert severity text to OTLP severity number.
fn severity_text_to_number(text: &str) -> Result<i32, String> {
    match text.to_uppercase().as_str() {
        "TRACE" => Ok(1),
        "DEBUG" => Ok(5),
        "INFO" => Ok(9),
        "WARN" | "WARNING" => Ok(13),
        "ERROR" => Ok(17),
        "FATAL" => Ok(21),
        _ => Err(format!(
            "unknown severity: {text}. Expected one of: TRACE, DEBUG, INFO, WARN, ERROR, FATAL"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_span_duration() {
        let rule = AlertRule::parse("span_duration > 5s").unwrap();
        assert!(matches!(
            rule.condition,
            AlertCondition::SpanDuration { threshold } if threshold == Duration::from_secs(5)
        ));
    }

    #[test]
    fn test_parse_span_duration_ms() {
        let rule = AlertRule::parse("span_duration > 500ms").unwrap();
        assert!(matches!(
            rule.condition,
            AlertCondition::SpanDuration { threshold } if threshold == Duration::from_millis(500)
        ));
    }

    #[test]
    fn test_parse_error_rate() {
        let rule = AlertRule::parse("error_rate > 10/min").unwrap();
        assert!(matches!(
            rule.condition,
            AlertCondition::ErrorRate { max_count: 10, window } if window == Duration::from_secs(60)
        ));
    }

    #[test]
    fn test_parse_error_rate_custom_window() {
        let rule = AlertRule::parse("error_rate > 5/5m").unwrap();
        assert!(matches!(
            rule.condition,
            AlertCondition::ErrorRate { max_count: 5, window } if window == Duration::from_secs(300)
        ));
    }

    #[test]
    fn test_parse_log_body_contains() {
        let rule = AlertRule::parse("log_body contains 'panic'").unwrap();
        assert!(matches!(
            rule.condition,
            AlertCondition::LogBodyContains { ref pattern } if pattern == "panic"
        ));
    }

    #[test]
    fn test_parse_log_severity() {
        let rule = AlertRule::parse("log_severity >= ERROR").unwrap();
        assert!(matches!(
            rule.condition,
            AlertCondition::LogSeverityAtLeast { min_severity: 17 }
        ));
    }

    #[test]
    fn test_parse_log_severity_warn() {
        let rule = AlertRule::parse("log_severity >= WARN").unwrap();
        assert!(matches!(
            rule.condition,
            AlertCondition::LogSeverityAtLeast { min_severity: 13 }
        ));
    }

    #[test]
    fn test_parse_metric_threshold() {
        let rule = AlertRule::parse("metric cpu.usage > 90.0").unwrap();
        match rule.condition {
            AlertCondition::MetricThreshold {
                ref metric_name,
                ref op,
                threshold,
            } => {
                assert_eq!(metric_name, "cpu.usage");
                assert!(matches!(op, CmpOp::Gt));
                assert!((threshold - 90.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected MetricThreshold"),
        }
    }

    #[test]
    fn test_parse_metric_threshold_lte() {
        let rule = AlertRule::parse("metric memory.free <= 100.0").unwrap();
        match rule.condition {
            AlertCondition::MetricThreshold {
                ref metric_name,
                ref op,
                threshold,
            } => {
                assert_eq!(metric_name, "memory.free");
                assert!(matches!(op, CmpOp::Lte));
                assert!((threshold - 100.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected MetricThreshold"),
        }
    }

    #[test]
    fn test_parse_invalid_rule() {
        assert!(AlertRule::parse("gibberish").is_err());
    }

    #[test]
    fn test_parse_invalid_duration() {
        assert!(AlertRule::parse("span_duration > abc").is_err());
    }

    #[test]
    fn test_parse_invalid_error_rate() {
        assert!(AlertRule::parse("error_rate > bad").is_err());
    }

    #[test]
    fn test_cmp_op_eval() {
        assert!(CmpOp::Gt.eval(10.0, 5.0));
        assert!(!CmpOp::Gt.eval(5.0, 10.0));
        assert!(CmpOp::Lt.eval(5.0, 10.0));
        assert!(CmpOp::Gte.eval(5.0, 5.0));
        assert!(CmpOp::Lte.eval(5.0, 5.0));
        assert!(CmpOp::Eq.eval(5.0, 5.0));
    }

    #[test]
    fn test_parse_duration_str() {
        assert_eq!(parse_duration_str("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(
            parse_duration_str("500ms").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(parse_duration_str("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration_str("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(
            parse_duration_str("1d").unwrap(),
            Duration::from_secs(86400)
        );
    }
}
