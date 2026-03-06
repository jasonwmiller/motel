use anyhow::Result;

use crate::cli::{DiffArgs, OutputFormat};
use crate::client::print_table;
use crate::diff::{DiffResult, SpanDiff, compute_diff};
use crate::query_proto::QueryTracesRequest;
use crate::query_proto::query_service_client::QueryServiceClient;
use crate::tui::app::SpanRow;

pub async fn run(args: DiffArgs) -> Result<()> {
    let mut client = QueryServiceClient::connect(args.addr.clone()).await?;

    // Fetch trace A
    let resp_a = client
        .query_traces(QueryTracesRequest {
            trace_id: args.trace_id_a.clone(),
            ..Default::default()
        })
        .await?
        .into_inner();

    // Fetch trace B
    let resp_b = client
        .query_traces(QueryTracesRequest {
            trace_id: args.trace_id_b.clone(),
            ..Default::default()
        })
        .await?
        .into_inner();

    let spans_a = flatten_response_spans(&resp_a.resource_spans);
    let spans_b = flatten_response_spans(&resp_b.resource_spans);

    if spans_a.is_empty() {
        anyhow::bail!("No spans found for trace A ({})", args.trace_id_a);
    }
    if spans_b.is_empty() {
        anyhow::bail!("No spans found for trace B ({})", args.trace_id_b);
    }

    let diff = compute_diff(&spans_a, &spans_b);

    match args.output {
        OutputFormat::Text => render_text(&args, &diff),
        OutputFormat::Table => render_table(&args, &diff),
        OutputFormat::Jsonl => render_jsonl(&diff)?,
        OutputFormat::Csv => render_csv(&diff)?,
    }

    Ok(())
}

fn flatten_response_spans(
    resource_spans: &[crate::otel::trace::v1::ResourceSpans],
) -> Vec<SpanRow> {
    let mut rows = Vec::new();
    for rs in resource_spans {
        let resource_attrs = rs
            .resource
            .as_ref()
            .map(|r| r.attributes.clone())
            .unwrap_or_default();
        let service = extract_service_name(&resource_attrs);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                let duration = span
                    .end_time_unix_nano
                    .saturating_sub(span.start_time_unix_nano);
                rows.push(SpanRow {
                    time_nano: span.start_time_unix_nano,
                    service_name: service.clone(),
                    span_name: span.name.clone(),
                    duration_ns: duration,
                    trace_id: span.trace_id.clone(),
                    span_id: span.span_id.clone(),
                    parent_span_id: span.parent_span_id.clone(),
                    kind: span.kind,
                    status_code: span.status.as_ref().map_or(0, |s| s.code),
                    status_message: span
                        .status
                        .as_ref()
                        .map_or_else(String::new, |s| s.message.clone()),
                    attributes: span.attributes.clone(),
                    resource_attributes: resource_attrs.clone(),
                    events_count: span.events.len(),
                    links_count: span.links.len(),
                });
            }
        }
    }
    rows
}

fn extract_service_name(attrs: &[crate::otel::common::v1::KeyValue]) -> String {
    for kv in attrs {
        if kv.key == "service.name" {
            if let Some(ref v) = kv.value {
                if let Some(crate::otel::common::v1::any_value::Value::StringValue(ref s)) = v.value
                {
                    return s.clone();
                }
            }
        }
    }
    "<unknown>".to_string()
}

fn format_duration_ms(ns: u64) -> String {
    format!("{:.3}ms", ns as f64 / 1_000_000.0)
}

fn short_trace_id(id: &str) -> &str {
    if id.len() > 8 { &id[..8] } else { id }
}

fn render_text(args: &DiffArgs, diff: &DiffResult) {
    let tid_a = short_trace_id(&args.trace_id_a);
    let tid_b = short_trace_id(&args.trace_id_b);

    let dur_a = format_duration_ms(diff.total_duration_a);
    let dur_b = format_duration_ms(diff.total_duration_b);
    let total_delta = diff.total_duration_b as i64 - diff.total_duration_a as i64;
    let total_pct = if diff.total_duration_a == 0 {
        0.0
    } else {
        (total_delta as f64 / diff.total_duration_a as f64) * 100.0
    };
    let sign = if total_pct >= 0.0 { "+" } else { "" };

    println!("Trace Diff: {} vs {}", tid_a, tid_b);
    println!(
        "Duration: {} -> {} ({}{:.1}%)",
        dur_a, dur_b, sign, total_pct
    );
    println!();

    for sd in &diff.span_diffs {
        match sd {
            SpanDiff::Matched {
                span_a,
                span_b,
                duration_pct_change,
                ..
            } => {
                let s = if *duration_pct_change >= 0.0 { "+" } else { "" };
                println!(
                    "  {:<30} {:<15} {} -> {} ({}{:.1}%)",
                    span_a.span_name,
                    span_a.service_name,
                    format_duration_ms(span_a.duration_ns),
                    format_duration_ms(span_b.duration_ns),
                    s,
                    duration_pct_change,
                );
            }
            SpanDiff::OnlyInA(span) => {
                println!(
                    "- {:<30} {:<15} {} -> -             removed",
                    span.span_name,
                    span.service_name,
                    format_duration_ms(span.duration_ns),
                );
            }
            SpanDiff::OnlyInB(span) => {
                println!(
                    "+ {:<30} {:<15} -             -> {} +new",
                    span.span_name,
                    span.service_name,
                    format_duration_ms(span.duration_ns),
                );
            }
        }
    }
}

fn render_table(args: &DiffArgs, diff: &DiffResult) {
    let tid_a = short_trace_id(&args.trace_id_a);
    let tid_b = short_trace_id(&args.trace_id_b);

    let dur_a = format_duration_ms(diff.total_duration_a);
    let dur_b = format_duration_ms(diff.total_duration_b);
    let total_delta = diff.total_duration_b as i64 - diff.total_duration_a as i64;
    let total_pct = if diff.total_duration_a == 0 {
        0.0
    } else {
        (total_delta as f64 / diff.total_duration_a as f64) * 100.0
    };
    let sign = if total_pct >= 0.0 { "+" } else { "" };

    println!("Trace Diff: {} vs {}", tid_a, tid_b);
    println!(
        "Duration: {} -> {} ({}{:.1}%)",
        dur_a, dur_b, sign, total_pct
    );
    println!();

    let headers = &["SPAN", "SERVICE", "A DURATION", "B DURATION", "CHANGE"];

    let mut col_span = Vec::new();
    let mut col_service = Vec::new();
    let mut col_dur_a = Vec::new();
    let mut col_dur_b = Vec::new();
    let mut col_change = Vec::new();

    for sd in &diff.span_diffs {
        match sd {
            SpanDiff::Matched {
                span_a,
                span_b,
                duration_pct_change,
                ..
            } => {
                col_span.push(span_a.span_name.clone());
                col_service.push(span_a.service_name.clone());
                col_dur_a.push(format_duration_ms(span_a.duration_ns));
                col_dur_b.push(format_duration_ms(span_b.duration_ns));
                let s = if *duration_pct_change >= 0.0 { "+" } else { "" };
                col_change.push(format!("{}{:.1}%", s, duration_pct_change));
            }
            SpanDiff::OnlyInA(span) => {
                col_span.push(format!("- {}", span.span_name));
                col_service.push(span.service_name.clone());
                col_dur_a.push(format_duration_ms(span.duration_ns));
                col_dur_b.push("-".to_string());
                col_change.push("removed".to_string());
            }
            SpanDiff::OnlyInB(span) => {
                col_span.push(format!("+ {}", span.span_name));
                col_service.push(span.service_name.clone());
                col_dur_a.push("-".to_string());
                col_dur_b.push(format_duration_ms(span.duration_ns));
                col_change.push("+new".to_string());
            }
        }
    }

    let columns = vec![col_span, col_service, col_dur_a, col_dur_b, col_change];
    print_table(headers, &columns);
}

fn render_jsonl(diff: &DiffResult) -> Result<()> {
    for sd in &diff.span_diffs {
        let obj = match sd {
            SpanDiff::Matched {
                span_a,
                span_b,
                duration_pct_change,
                ..
            } => serde_json::json!({
                "span_name": span_a.span_name,
                "service": span_a.service_name,
                "duration_a_ms": span_a.duration_ns as f64 / 1_000_000.0,
                "duration_b_ms": span_b.duration_ns as f64 / 1_000_000.0,
                "change_pct": (duration_pct_change * 10.0).round() / 10.0,
                "status": "matched",
            }),
            SpanDiff::OnlyInA(span) => serde_json::json!({
                "span_name": span.span_name,
                "service": span.service_name,
                "duration_a_ms": span.duration_ns as f64 / 1_000_000.0,
                "duration_b_ms": null,
                "change_pct": null,
                "status": "removed",
            }),
            SpanDiff::OnlyInB(span) => serde_json::json!({
                "span_name": span.span_name,
                "service": span.service_name,
                "duration_a_ms": null,
                "duration_b_ms": span.duration_ns as f64 / 1_000_000.0,
                "change_pct": null,
                "status": "added",
            }),
        };
        println!("{}", serde_json::to_string(&obj)?);
    }
    Ok(())
}

fn render_csv(diff: &DiffResult) -> Result<()> {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());
    wtr.write_record([
        "span_name",
        "service",
        "duration_a_ms",
        "duration_b_ms",
        "change_pct",
        "status",
    ])?;

    for sd in &diff.span_diffs {
        match sd {
            SpanDiff::Matched {
                span_a,
                span_b,
                duration_pct_change,
                ..
            } => {
                wtr.write_record([
                    &span_a.span_name,
                    &span_a.service_name,
                    &format!("{:.3}", span_a.duration_ns as f64 / 1_000_000.0),
                    &format!("{:.3}", span_b.duration_ns as f64 / 1_000_000.0),
                    &format!("{:.1}", duration_pct_change),
                    "matched",
                ])?;
            }
            SpanDiff::OnlyInA(span) => {
                wtr.write_record([
                    &span.span_name,
                    &span.service_name,
                    &format!("{:.3}", span.duration_ns as f64 / 1_000_000.0),
                    "",
                    "",
                    "removed",
                ])?;
            }
            SpanDiff::OnlyInB(span) => {
                wtr.write_record([
                    &span.span_name,
                    &span.service_name,
                    "",
                    &format!("{:.3}", span.duration_ns as f64 / 1_000_000.0),
                    "",
                    "added",
                ])?;
            }
        }
    }
    wtr.flush()?;
    Ok(())
}
