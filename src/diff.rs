//! Shared diff computation logic for comparing two traces.
//!
//! Used by both the CLI `diff` subcommand and the TUI diff view.

use std::collections::HashMap;

use crate::tui::app::SpanRow;

/// Result of comparing two traces.
#[derive(Clone)]
pub struct DiffResult {
    pub span_diffs: Vec<SpanDiff>,
    pub total_duration_a: u64,
    pub total_duration_b: u64,
}

/// A single span comparison entry.
#[derive(Clone)]
pub enum SpanDiff {
    /// Span exists in both traces, matched by (service_name, span_name).
    Matched {
        span_a: SpanRow,
        span_b: SpanRow,
        duration_delta_ns: i64,
        duration_pct_change: f64,
    },
    /// Span only in trace A (removed).
    OnlyInA(SpanRow),
    /// Span only in trace B (added).
    OnlyInB(SpanRow),
}

/// Compute the diff between two sets of spans.
///
/// Matching algorithm:
/// 1. Build a map keyed by (service_name, span_name) for each trace.
/// 2. For matching keys, pair spans positionally (1st with 1st, etc.).
/// 3. Unmatched spans become OnlyInA / OnlyInB.
/// 4. Result is sorted: matched first (by span_a start time), then OnlyInA, then OnlyInB.
pub fn compute_diff(spans_a: &[SpanRow], spans_b: &[SpanRow]) -> DiffResult {
    let total_duration_a = compute_total_duration(spans_a);
    let total_duration_b = compute_total_duration(spans_b);

    let mut map_a: HashMap<(String, String), Vec<&SpanRow>> = HashMap::new();
    for span in spans_a {
        map_a
            .entry((span.service_name.clone(), span.span_name.clone()))
            .or_default()
            .push(span);
    }

    let mut map_b: HashMap<(String, String), Vec<&SpanRow>> = HashMap::new();
    for span in spans_b {
        map_b
            .entry((span.service_name.clone(), span.span_name.clone()))
            .or_default()
            .push(span);
    }

    let mut matched = Vec::new();
    let mut only_a = Vec::new();
    let mut only_b = Vec::new();

    // All keys from both maps
    let mut all_keys: Vec<(String, String)> = map_a.keys().cloned().collect();
    for key in map_b.keys() {
        if !map_a.contains_key(key) {
            all_keys.push(key.clone());
        }
    }

    for key in &all_keys {
        let a_spans = map_a.remove(key).unwrap_or_default();
        let b_spans = map_b.remove(key).unwrap_or_default();

        let common_len = a_spans.len().min(b_spans.len());

        for i in 0..common_len {
            let sa = a_spans[i];
            let sb = b_spans[i];
            let delta = sb.duration_ns as i64 - sa.duration_ns as i64;
            let pct = if sa.duration_ns == 0 {
                if sb.duration_ns == 0 { 0.0 } else { 100.0 }
            } else {
                (delta as f64 / sa.duration_ns as f64) * 100.0
            };
            matched.push(SpanDiff::Matched {
                span_a: sa.clone(),
                span_b: sb.clone(),
                duration_delta_ns: delta,
                duration_pct_change: pct,
            });
        }

        for span in a_spans.iter().skip(common_len) {
            only_a.push(SpanDiff::OnlyInA((*span).clone()));
        }
        for span in b_spans.iter().skip(common_len) {
            only_b.push(SpanDiff::OnlyInB((*span).clone()));
        }
    }

    // Sort matched by span_a start time
    matched.sort_by_key(|d| match d {
        SpanDiff::Matched { span_a, .. } => span_a.time_nano,
        _ => 0,
    });

    // Sort only_a and only_b by start time
    only_a.sort_by_key(|d| match d {
        SpanDiff::OnlyInA(s) => s.time_nano,
        _ => 0,
    });
    only_b.sort_by_key(|d| match d {
        SpanDiff::OnlyInB(s) => s.time_nano,
        _ => 0,
    });

    let mut span_diffs = matched;
    span_diffs.append(&mut only_a);
    span_diffs.append(&mut only_b);

    DiffResult {
        span_diffs,
        total_duration_a,
        total_duration_b,
    }
}

fn compute_total_duration(spans: &[SpanRow]) -> u64 {
    if spans.is_empty() {
        return 0;
    }
    let min_start = spans.iter().map(|s| s.time_nano).min().unwrap_or(0);
    let max_end = spans
        .iter()
        .map(|s| s.time_nano + s.duration_ns)
        .max()
        .unwrap_or(0);
    max_end.saturating_sub(min_start)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::app::SpanRow;

    fn make_span(service: &str, name: &str, duration_ns: u64, time_nano: u64) -> SpanRow {
        SpanRow {
            time_nano,
            service_name: service.to_string(),
            span_name: name.to_string(),
            duration_ns,
            trace_id: vec![1, 2, 3, 4],
            span_id: vec![5, 6, 7, 8],
            parent_span_id: vec![],
            kind: 1,
            status_code: 0,
            status_message: String::new(),
            attributes: vec![],
            resource_attributes: vec![],
            events_count: 0,
            links_count: 0,
        }
    }

    #[test]
    fn test_diff_matched_spans() {
        let spans_a = vec![make_span("svc", "GET /api", 100_000_000, 1000)];
        let spans_b = vec![make_span("svc", "GET /api", 120_000_000, 2000)];

        let result = compute_diff(&spans_a, &spans_b);
        assert_eq!(result.span_diffs.len(), 1);

        match &result.span_diffs[0] {
            SpanDiff::Matched {
                duration_delta_ns,
                duration_pct_change,
                ..
            } => {
                assert_eq!(*duration_delta_ns, 20_000_000);
                assert!((duration_pct_change - 20.0).abs() < 0.01);
            }
            _ => panic!("Expected Matched"),
        }
    }

    #[test]
    fn test_diff_added_removed_spans() {
        let spans_a = vec![make_span("svc", "auth.check", 8_000_000, 1000)];
        let spans_b = vec![make_span("svc", "cache.miss", 12_000_000, 2000)];

        let result = compute_diff(&spans_a, &spans_b);
        assert_eq!(result.span_diffs.len(), 2);

        let has_only_a = result
            .span_diffs
            .iter()
            .any(|d| matches!(d, SpanDiff::OnlyInA(s) if s.span_name == "auth.check"));
        let has_only_b = result
            .span_diffs
            .iter()
            .any(|d| matches!(d, SpanDiff::OnlyInB(s) if s.span_name == "cache.miss"));

        assert!(has_only_a);
        assert!(has_only_b);
    }

    #[test]
    fn test_diff_multiple_same_name_spans() {
        let spans_a = vec![
            make_span("svc", "db.query", 30_000_000, 1000),
            make_span("svc", "db.query", 40_000_000, 2000),
            make_span("svc", "db.query", 50_000_000, 3000),
        ];
        let spans_b = vec![
            make_span("svc", "db.query", 35_000_000, 1000),
            make_span("svc", "db.query", 45_000_000, 2000),
            make_span("svc", "db.query", 55_000_000, 3000),
        ];

        let result = compute_diff(&spans_a, &spans_b);
        assert_eq!(result.span_diffs.len(), 3);

        for diff in &result.span_diffs {
            match diff {
                SpanDiff::Matched {
                    duration_delta_ns, ..
                } => {
                    assert_eq!(*duration_delta_ns, 5_000_000);
                }
                _ => panic!("Expected all Matched"),
            }
        }
    }

    #[test]
    fn test_diff_empty_trace() {
        let spans_a: Vec<SpanRow> = vec![];
        let spans_b = vec![
            make_span("svc", "span1", 10_000, 1000),
            make_span("svc", "span2", 20_000, 2000),
        ];

        let result = compute_diff(&spans_a, &spans_b);
        assert_eq!(result.span_diffs.len(), 2);
        assert_eq!(result.total_duration_a, 0);

        for diff in &result.span_diffs {
            assert!(matches!(diff, SpanDiff::OnlyInB(_)));
        }
    }

    #[test]
    fn test_diff_duration_percentage() {
        // 100ns -> 120ns = +20%
        let spans_a = vec![make_span("svc", "op", 100, 1000)];
        let spans_b = vec![make_span("svc", "op", 120, 2000)];
        let result = compute_diff(&spans_a, &spans_b);
        match &result.span_diffs[0] {
            SpanDiff::Matched {
                duration_pct_change,
                ..
            } => {
                assert!((duration_pct_change - 20.0).abs() < 0.01);
            }
            _ => panic!("Expected Matched"),
        }

        // 100ns -> 80ns = -20%
        let spans_a = vec![make_span("svc", "op", 100, 1000)];
        let spans_b = vec![make_span("svc", "op", 80, 2000)];
        let result = compute_diff(&spans_a, &spans_b);
        match &result.span_diffs[0] {
            SpanDiff::Matched {
                duration_pct_change,
                ..
            } => {
                assert!((duration_pct_change - (-20.0)).abs() < 0.01);
            }
            _ => panic!("Expected Matched"),
        }
    }
}
