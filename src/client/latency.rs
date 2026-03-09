use anyhow::Result;

use crate::cli::{LatencyArgs, OutputFormat};
use crate::client::extract_request_trace_id;
use crate::query_proto::SqlQueryRequest;
use crate::query_proto::query_service_client::QueryServiceClient;

pub async fn run(args: LatencyArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    // Build SQL to fetch durations
    let mut conditions = vec![format!("span_name = '{}'", escape_sql(&args.span_name))];
    if let Some(ref service) = args.service {
        conditions.push(format!("service_name = '{}'", escape_sql(service)));
    }
    if let Some(ref since) = args.since {
        let dt = crate::client::parse_time_spec(since)?;
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        conditions.push(format!("start_time_unix_nano >= {}", nanos));
    }

    let sql = format!(
        "SELECT duration_ns FROM traces WHERE {} ORDER BY duration_ns ASC",
        conditions.join(" AND ")
    );

    let request = SqlQueryRequest { query: sql };
    let response = client.sql_query(request).await?;

    if args.show_trace_id
        && let Some(trace_id) = extract_request_trace_id(&response)
    {
        eprintln!("trace_id: {}", trace_id);
    }

    let resp = response.into_inner();

    // Parse duration values from rows (values are strings from SQL response)
    let mut durations: Vec<f64> = Vec::new();
    for row in &resp.rows {
        if let Some(val) = row.values.first() {
            // duration_ns comes back as a string representation of an i64
            if let Ok(ns) = val.parse::<i64>() {
                durations.push(ns as f64 / 1_000_000.0);
            } else if let Ok(ns) = val.parse::<f64>() {
                durations.push(ns / 1_000_000.0);
            }
        }
    }

    if durations.is_empty() {
        println!("No spans found for '{}'.", args.span_name);
        return Ok(());
    }

    durations.sort_by(|a, b| a.partial_cmp(b).unwrap());

    match args.output {
        OutputFormat::Text | OutputFormat::Table => {
            render_histogram(&args.span_name, &durations, args.buckets);
        }
        OutputFormat::Jsonl => render_jsonl(&args.span_name, &durations),
        OutputFormat::Csv => render_csv(&args.span_name, &durations),
    }

    Ok(())
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn render_histogram(span_name: &str, durations: &[f64], bucket_count: usize) {
    let min = durations[0];
    let max = durations[durations.len() - 1];
    let p50 = percentile(durations, 50.0);
    let p95 = percentile(durations, 95.0);
    let p99 = percentile(durations, 99.0);
    let mean = durations.iter().sum::<f64>() / durations.len() as f64;

    // Header
    println!("Latency Distribution: {}", span_name);
    println!(
        "  Samples: {}  Min: {:.3}ms  Max: {:.3}ms  Mean: {:.3}ms",
        durations.len(),
        min,
        max,
        mean
    );
    println!(
        "  p50: {:.3}ms  p95: {:.3}ms  p99: {:.3}ms\n",
        p50, p95, p99
    );

    // Handle identical durations
    let range = max - min;
    if range == 0.0 {
        println!(
            "  All {} samples have identical duration: {:.3}ms",
            durations.len(),
            min
        );
        return;
    }

    let bucket_width = range / bucket_count as f64;
    let mut buckets: Vec<usize> = vec![0; bucket_count];

    for &d in durations {
        let idx = ((d - min) / bucket_width) as usize;
        let idx = idx.min(bucket_count - 1);
        buckets[idx] += 1;
    }

    let max_count = *buckets.iter().max().unwrap_or(&1);
    let bar_max_width = 50;

    // Render buckets
    for (i, &count) in buckets.iter().enumerate() {
        let lo = min + i as f64 * bucket_width;
        let hi = lo + bucket_width;
        let bar_len = if max_count > 0 {
            (count as f64 / max_count as f64 * bar_max_width as f64) as usize
        } else {
            0
        };
        let bar: String = "\u{2588}".repeat(bar_len);
        println!("  {:>8.2}ms - {:>8.2}ms [{:>5}] {}", lo, hi, count, bar);
    }
}

fn render_jsonl(span_name: &str, durations: &[f64]) {
    for d in durations {
        println!(
            "{}",
            serde_json::json!({"span_name": span_name, "duration_ms": d})
        );
    }
}

fn render_csv(span_name: &str, durations: &[f64]) {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());
    wtr.write_record(["span_name", "duration_ms"]).unwrap();
    for d in durations {
        wtr.write_record([span_name, &format!("{:.3}", d)]).unwrap();
    }
    wtr.flush().unwrap();
}

/// Basic SQL string escaping (single quotes).
fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_basic() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert!((percentile(&data, 50.0) - 5.5).abs() < 1.0);
        assert!((percentile(&data, 99.0) - 10.0).abs() < 0.5);
    }

    #[test]
    fn test_percentile_single_value() {
        let data = vec![42.0];
        assert_eq!(percentile(&data, 50.0), 42.0);
        assert_eq!(percentile(&data, 99.0), 42.0);
    }

    #[test]
    fn test_percentile_empty() {
        let data: Vec<f64> = vec![];
        assert_eq!(percentile(&data, 50.0), 0.0);
    }

    #[test]
    fn test_escape_sql() {
        assert_eq!(escape_sql("hello"), "hello");
        assert_eq!(escape_sql("it's"), "it''s");
    }

    #[test]
    fn test_histogram_bucket_assignment() {
        // Verify that bucket assignment distributes values correctly
        // 10 values from 1.0 to 10.0 with 5 buckets
        // bucket_width = (10-1)/5 = 1.8
        // bucket 0: [1.0, 2.8) => 1.0, 2.0
        // bucket 1: [2.8, 4.6) => 3.0, 4.0
        // bucket 2: [4.6, 6.4) => 5.0, 6.0
        // bucket 3: [6.4, 8.2) => 7.0, 8.0
        // bucket 4: [8.2, 10.0] => 9.0, 10.0
        let durations = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let bucket_count = 5;
        let min = 1.0;
        let max = 10.0;
        let bucket_width = (max - min) / bucket_count as f64;
        let mut buckets: Vec<usize> = vec![0; bucket_count];
        for &d in &durations {
            let idx = ((d - min) / bucket_width) as usize;
            let idx = idx.min(bucket_count - 1);
            buckets[idx] += 1;
        }
        // Each bucket should have 2 values
        assert_eq!(buckets, vec![2, 2, 2, 2, 2]);
    }
}
