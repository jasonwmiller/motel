pub mod clear;
pub mod init;
pub mod import;
pub mod export;
pub mod log;
pub mod metrics;
pub mod service_map;
pub mod replay;
pub mod shutdown;
pub mod sql;
pub mod status;
pub mod trace;
pub mod view;

use chrono::{DateTime, Utc};

/// Parse a time spec: relative (30s, 5m, 1h, 2d) or RFC3339 absolute
pub fn parse_time_spec(spec: &str) -> anyhow::Result<DateTime<Utc>> {
    // Try RFC3339 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(spec) {
        return Ok(dt.with_timezone(&Utc));
    }
    // Try relative time
    let (num_str, unit) = spec.split_at(spec.len().saturating_sub(1));
    let num: i64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid time spec: {}", spec))?;
    let duration = match unit {
        "s" => chrono::Duration::seconds(num),
        "m" => chrono::Duration::minutes(num),
        "h" => chrono::Duration::hours(num),
        "d" => chrono::Duration::days(num),
        _ => anyhow::bail!("Invalid time unit in: {}", spec),
    };
    Ok(Utc::now() - duration)
}

/// Encode bytes as hex string
pub fn hex_encode(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Decode hex string to bytes
pub fn hex_decode(s: &str) -> anyhow::Result<Vec<u8>> {
    hex::decode(s).map_err(|e| anyhow::anyhow!("Invalid hex: {}", e))
}

/// Parse key=value attribute filter
pub fn parse_attributes(attrs: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    attrs
        .iter()
        .map(|a| {
            let (k, v) = a
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("Attribute must be key=value: {}", a))?;
            Ok((k.to_string(), v.to_string()))
        })
        .collect()
}

/// Try to extract a trace-id from gRPC response metadata (traceparent header).
pub fn extract_request_trace_id<T>(response: &tonic::Response<T>) -> Option<String> {
    response
        .metadata()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(|tp| {
            // traceparent format: version-trace_id-parent_id-flags
            let parts: Vec<&str> = tp.split('-').collect();
            if parts.len() >= 2 {
                Some(parts[1].to_string())
            } else {
                None
            }
        })
}

/// Print a table with aligned columns.
/// `headers` is a slice of column header names.
/// `columns` is a Vec of columns, where each column is a Vec of cell values.
pub fn print_table(headers: &[&str], columns: &[Vec<String>]) {
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let max_data = columns
                .get(i)
                .map(|col| col.iter().map(|s| s.len()).max().unwrap_or(0))
                .unwrap_or(0);
            h.len().max(max_data)
        })
        .collect();

    // Header
    let header_line: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:<width$}", h, width = widths[i]))
        .collect();
    println!("{}", header_line.join("  "));

    // Separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("  "));

    // Rows
    let row_count = columns.first().map(|c| c.len()).unwrap_or(0);
    for row_idx in 0..row_count {
        let line: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(col_idx, col)| {
                let val = col.get(row_idx).map(|s| s.as_str()).unwrap_or("");
                format!("{:<width$}", val, width = widths[col_idx])
            })
            .collect();
        println!("{}", line.join("  "));
    }
}
