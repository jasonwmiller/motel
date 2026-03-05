use crate::cli::{LogsArgs, MetricsArgs, TracesArgs};
use crate::client::parse_time_spec;

/// Convert TracesArgs CLI flags to a SQL SELECT query.
pub fn traces_args_to_sql(args: &TracesArgs) -> String {
    let mut conditions = Vec::new();

    if let Some(ref service) = args.service {
        conditions.push(format!("service_name = '{}'", escape_sql(service)));
    }
    if let Some(ref span_name) = args.span_name {
        conditions.push(format!("span_name = '{}'", escape_sql(span_name)));
    }
    if let Some(ref trace_id) = args.trace_id {
        conditions.push(format!("trace_id = '{}'", escape_sql(trace_id)));
    }
    if let Some(ref since) = args.since
        && let Ok(dt) = parse_time_spec(since)
    {
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        conditions.push(format!("start_time_unix_nano >= {}", nanos));
    }
    if let Some(ref until) = args.until
        && let Ok(dt) = parse_time_spec(until)
    {
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        conditions.push(format!("start_time_unix_nano <= {}", nanos));
    }
    for attr in &args.attribute {
        // Each attribute is "key=value"; we search the JSON attributes column
        if let Some((key, value)) = attr.split_once('=') {
            conditions.push(format!(
                "attributes LIKE '%\"{}\":\"{}\"%'",
                escape_sql(key),
                escape_sql(value)
            ));
        }
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let limit_clause = args
        .limit
        .map(|n| format!(" LIMIT {}", n))
        .unwrap_or_default();

    format!(
        "SELECT * FROM traces{} ORDER BY start_time_unix_nano DESC{}",
        where_clause, limit_clause
    )
}

/// Convert LogsArgs CLI flags to a SQL SELECT query.
pub fn logs_args_to_sql(args: &LogsArgs) -> String {
    let mut conditions = Vec::new();

    if let Some(ref service) = args.service {
        conditions.push(format!("service_name = '{}'", escape_sql(service)));
    }
    if let Some(ref severity) = args.severity {
        conditions.push(format!("severity_text LIKE '%{}%'", escape_sql(severity)));
    }
    if let Some(ref body) = args.body {
        conditions.push(format!("body LIKE '%{}%'", escape_sql(body)));
    }
    if let Some(ref since) = args.since
        && let Ok(dt) = parse_time_spec(since)
    {
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        conditions.push(format!("timestamp_unix_nano >= {}", nanos));
    }
    if let Some(ref until) = args.until
        && let Ok(dt) = parse_time_spec(until)
    {
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        conditions.push(format!("timestamp_unix_nano <= {}", nanos));
    }
    for attr in &args.attribute {
        if let Some((key, value)) = attr.split_once('=') {
            conditions.push(format!(
                "attributes LIKE '%\"{}\":\"{}\"%'",
                escape_sql(key),
                escape_sql(value)
            ));
        }
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let limit_clause = args
        .limit
        .map(|n| format!(" LIMIT {}", n))
        .unwrap_or_default();

    format!(
        "SELECT * FROM logs{} ORDER BY timestamp_unix_nano DESC{}",
        where_clause, limit_clause
    )
}

/// Convert MetricsArgs CLI flags to a SQL SELECT query.
pub fn metrics_args_to_sql(args: &MetricsArgs) -> String {
    let mut conditions = Vec::new();

    if let Some(ref service) = args.service {
        conditions.push(format!("service_name = '{}'", escape_sql(service)));
    }
    if let Some(ref name) = args.name {
        conditions.push(format!("metric_name = '{}'", escape_sql(name)));
    }
    if let Some(ref since) = args.since
        && let Ok(dt) = parse_time_spec(since)
    {
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        conditions.push(format!("timestamp_unix_nano >= {}", nanos));
    }
    if let Some(ref until) = args.until
        && let Ok(dt) = parse_time_spec(until)
    {
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        conditions.push(format!("timestamp_unix_nano <= {}", nanos));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let limit_clause = args
        .limit
        .map(|n| format!(" LIMIT {}", n))
        .unwrap_or_default();

    format!(
        "SELECT * FROM metrics{} ORDER BY timestamp_unix_nano DESC{}",
        where_clause, limit_clause
    )
}

/// Basic SQL string escaping (single quotes).
fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traces_args_basic() {
        let args = TracesArgs {
            service: Some("my-service".into()),
            span_name: None,
            trace_id: None,
            since: None,
            until: None,
            limit: Some(10),
            attribute: vec![],
            output: crate::cli::OutputFormat::Text,
            show_trace_id: false,
            addr: "http://localhost:4319".into(),
        };
        let sql = traces_args_to_sql(&args);
        assert!(sql.contains("service_name = 'my-service'"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("ORDER BY start_time_unix_nano DESC"));
    }

    #[test]
    fn test_traces_args_no_filters() {
        let args = TracesArgs {
            service: None,
            span_name: None,
            trace_id: None,
            since: None,
            until: None,
            limit: None,
            attribute: vec![],
            output: crate::cli::OutputFormat::Text,
            show_trace_id: false,
            addr: "http://localhost:4319".into(),
        };
        let sql = traces_args_to_sql(&args);
        assert_eq!(
            sql,
            "SELECT * FROM traces ORDER BY start_time_unix_nano DESC"
        );
    }

    #[test]
    fn test_logs_args_with_severity() {
        let args = LogsArgs {
            service: None,
            severity: Some("ERROR".into()),
            body: None,
            since: None,
            until: None,
            limit: None,
            attribute: vec![],
            output: crate::cli::OutputFormat::Text,
            show_trace_id: false,
            addr: "http://localhost:4319".into(),
        };
        let sql = logs_args_to_sql(&args);
        assert!(sql.contains("severity_text LIKE '%ERROR%'"));
    }

    #[test]
    fn test_metrics_args_with_name() {
        let args = MetricsArgs {
            service: None,
            name: Some("cpu.usage".into()),
            since: None,
            until: None,
            limit: Some(5),
            output: crate::cli::OutputFormat::Text,
            show_trace_id: false,
            addr: "http://localhost:4319".into(),
        };
        let sql = metrics_args_to_sql(&args);
        assert!(sql.contains("metric_name = 'cpu.usage'"));
        assert!(sql.contains("LIMIT 5"));
    }

    #[test]
    fn test_attribute_filter() {
        let args = TracesArgs {
            service: None,
            span_name: None,
            trace_id: None,
            since: None,
            until: None,
            limit: None,
            attribute: vec!["http.method=GET".into()],
            output: crate::cli::OutputFormat::Text,
            show_trace_id: false,
            addr: "http://localhost:4319".into(),
        };
        let sql = traces_args_to_sql(&args);
        assert!(sql.contains(r#"attributes LIKE '%"http.method":"GET"%'"#));
    }
}
