use std::collections::HashSet;

use anyhow::Result;

use crate::cli::{ServiceMapArgs, ServiceMapFormat};
use crate::client::extract_request_trace_id;
use crate::query_proto::SqlQueryRequest;
use crate::query_proto::query_service_client::QueryServiceClient;

/// A parsed edge from the SQL result: (caller, callee, call_count, avg_duration_ms).
type Edge = (String, String, u64, f64);

pub async fn run(args: ServiceMapArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    let since_clause = if let Some(ref since) = args.since {
        let dt = crate::client::parse_time_spec(since)?;
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        format!(
            " AND t1.start_time_unix_nano >= {} AND t2.start_time_unix_nano >= {}",
            nanos, nanos
        )
    } else {
        String::new()
    };

    let sql = format!(
        "SELECT t1.service_name as caller_service, t2.service_name as callee_service, \
         COUNT(*) as call_count, AVG(t2.duration_ns) as avg_duration_ns \
         FROM traces t1 JOIN traces t2 ON t1.span_id = t2.parent_span_id \
         WHERE t1.service_name != t2.service_name{} \
         GROUP BY t1.service_name, t2.service_name \
         ORDER BY call_count DESC",
        since_clause
    );

    let request = SqlQueryRequest { query: sql };

    let response = client.sql_query(request).await?;

    if args.show_trace_id
        && let Some(trace_id) = extract_request_trace_id(&response)
    {
        eprintln!("trace_id: {}", trace_id);
    }

    let resp = response.into_inner();

    if resp.rows.is_empty() {
        println!("No cross-service calls found.");
        return Ok(());
    }

    let mut edges: Vec<Edge> = Vec::new();
    for row in &resp.rows {
        let caller = row.values.first().cloned().unwrap_or_default();
        let callee = row.values.get(1).cloned().unwrap_or_default();
        let call_count: u64 = row.values.get(2).and_then(|v| v.parse().ok()).unwrap_or(0);
        let avg_duration_ns: f64 = row
            .values
            .get(3)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0);
        let avg_duration_ms = avg_duration_ns / 1_000_000.0;
        edges.push((caller, callee, call_count, avg_duration_ms));
    }

    match args.format {
        ServiceMapFormat::Ascii => render_ascii(&edges),
        ServiceMapFormat::Mermaid => render_mermaid(&edges),
    }

    Ok(())
}

fn render_ascii(edges: &[Edge]) {
    let services: HashSet<&str> = edges
        .iter()
        .flat_map(|(from, to, _, _)| vec![from.as_str(), to.as_str()])
        .collect();

    println!("Service Dependency Map");
    println!("======================\n");

    for (from, to, count, avg_ms) in edges {
        println!(
            "  {} --({} calls, avg {:.1}ms)--> {}",
            from, count, avg_ms, to
        );
    }

    let mut sorted_services: Vec<&str> = services.into_iter().collect();
    sorted_services.sort();
    println!("\nServices: {}", sorted_services.join(", "));
}

fn render_mermaid(edges: &[Edge]) {
    println!("graph LR");
    for (from, to, count, avg_ms) in edges {
        let from_id = sanitize_mermaid_id(from);
        let to_id = sanitize_mermaid_id(to);
        println!(
            "    {}[\"{}\"] -->|{} calls, {:.1}ms avg| {}[\"{}\"]",
            from_id, from, count, avg_ms, to_id, to
        );
    }
}

fn sanitize_mermaid_id(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_ascii_basic() {
        let edges = vec![
            ("frontend".to_string(), "api".to_string(), 100, 45.2),
            ("api".to_string(), "database".to_string(), 50, 12.5),
        ];
        // Verify no panic
        render_ascii(&edges);
    }

    #[test]
    fn test_render_mermaid_basic() {
        let edges = vec![("frontend".to_string(), "api".to_string(), 100, 45.2)];
        // Verify no panic
        render_mermaid(&edges);
    }

    #[test]
    fn test_sanitize_mermaid_id() {
        assert_eq!(sanitize_mermaid_id("my-service.v2"), "my_service_v2");
        assert_eq!(sanitize_mermaid_id("simple"), "simple");
        assert_eq!(sanitize_mermaid_id("a/b:c"), "a_b_c");
    }

    #[test]
    fn test_render_ascii_empty() {
        render_ascii(&[]);
    }

    #[test]
    fn test_render_mermaid_empty() {
        render_mermaid(&[]);
    }
}
