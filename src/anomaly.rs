use std::collections::{HashMap, HashSet};

use crate::tui::app::SpanRow;

/// Default number of standard deviations above the mean to flag as an outlier.
pub const DEFAULT_STDDEV_THRESHOLD: f64 = 2.0;

/// Detect outlier spans by grouping spans by (service_name, span_name),
/// computing mean/stddev of duration_ns per group, and flagging spans
/// with duration > mean + threshold * stddev.
///
/// Returns a `HashSet` of span_id bytes for all outlier spans.
pub fn detect_outliers(spans: &[SpanRow], threshold: f64) -> HashSet<Vec<u8>> {
    let mut outliers = HashSet::new();

    // Group spans by (service_name, span_name)
    let mut groups: HashMap<(&str, &str), Vec<&SpanRow>> = HashMap::new();
    for span in spans {
        groups
            .entry((&span.service_name, &span.span_name))
            .or_default()
            .push(span);
    }

    for group_spans in groups.values() {
        // Need at least 2 spans to compute meaningful statistics
        if group_spans.len() < 2 {
            continue;
        }

        let durations: Vec<f64> = group_spans.iter().map(|s| s.duration_ns as f64).collect();
        let n = durations.len() as f64;
        let mean = durations.iter().sum::<f64>() / n;
        let variance = durations.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / n;
        let stddev = variance.sqrt();

        // If stddev is zero, all spans have the same duration — no outliers
        if stddev == 0.0 {
            continue;
        }

        let cutoff = mean + threshold * stddev;
        for span in group_spans {
            if (span.duration_ns as f64) > cutoff {
                outliers.insert(span.span_id.clone());
            }
        }
    }

    outliers
}

/// Check whether any span in a trace group is an outlier.
pub fn trace_has_outlier(span_ids: &[Vec<u8>], outlier_set: &HashSet<Vec<u8>>) -> bool {
    span_ids.iter().any(|id| outlier_set.contains(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::app::SpanRow;

    fn make_span(service: &str, name: &str, duration_ns: u64, span_id: Vec<u8>) -> SpanRow {
        SpanRow {
            time_nano: 1_000_000_000,
            service_name: service.to_string(),
            span_name: name.to_string(),
            duration_ns,
            trace_id: vec![1; 16],
            span_id,
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
    fn test_detect_outliers_basic() {
        let spans = vec![
            make_span("svc", "op", 100, vec![1]),
            make_span("svc", "op", 110, vec![2]),
            make_span("svc", "op", 105, vec![3]),
            make_span("svc", "op", 95, vec![4]),
            make_span("svc", "op", 100, vec![6]),
            make_span("svc", "op", 105, vec![7]),
            make_span("svc", "op", 98, vec![8]),
            make_span("svc", "op", 102, vec![9]),
            make_span("svc", "op", 5000, vec![5]), // clear outlier
        ];
        let outliers = detect_outliers(&spans, DEFAULT_STDDEV_THRESHOLD);
        assert!(outliers.contains(&vec![5u8]), "span 5 should be an outlier");
        assert!(
            !outliers.contains(&vec![1u8]),
            "span 1 should not be an outlier"
        );
    }

    #[test]
    fn test_fewer_than_two_spans() {
        let spans = vec![make_span("svc", "op", 100, vec![1])];
        let outliers = detect_outliers(&spans, DEFAULT_STDDEV_THRESHOLD);
        assert!(
            outliers.is_empty(),
            "single span should not produce outliers"
        );
    }

    #[test]
    fn test_zero_stddev() {
        let spans = vec![
            make_span("svc", "op", 100, vec![1]),
            make_span("svc", "op", 100, vec![2]),
            make_span("svc", "op", 100, vec![3]),
        ];
        let outliers = detect_outliers(&spans, DEFAULT_STDDEV_THRESHOLD);
        assert!(
            outliers.is_empty(),
            "identical durations should produce no outliers"
        );
    }

    #[test]
    fn test_separate_groups() {
        // Two different span groups: outlier only in one group
        let spans = vec![
            make_span("svc", "op-a", 100, vec![1]),
            make_span("svc", "op-a", 110, vec![2]),
            make_span("svc", "op-a", 105, vec![3]),
            make_span("svc", "op-a", 98, vec![8]),
            make_span("svc", "op-a", 102, vec![9]),
            make_span("svc", "op-a", 107, vec![10]),
            make_span("svc", "op-a", 5000, vec![4]), // clear outlier in op-a
            make_span("svc", "op-b", 500, vec![5]),
            make_span("svc", "op-b", 510, vec![6]),
            make_span("svc", "op-b", 505, vec![7]),
        ];
        let outliers = detect_outliers(&spans, DEFAULT_STDDEV_THRESHOLD);
        assert!(outliers.contains(&vec![4u8]));
        assert!(!outliers.contains(&vec![5u8]));
        assert!(!outliers.contains(&vec![6u8]));
    }

    #[test]
    fn test_trace_has_outlier() {
        let mut outlier_set = HashSet::new();
        outlier_set.insert(vec![5u8]);

        assert!(trace_has_outlier(
            &[vec![1], vec![5], vec![3]],
            &outlier_set
        ));
        assert!(!trace_has_outlier(&[vec![1], vec![2]], &outlier_set));
    }

    #[test]
    fn test_empty_spans() {
        let outliers = detect_outliers(&[], DEFAULT_STDDEV_THRESHOLD);
        assert!(outliers.is_empty());
    }

    #[test]
    fn test_custom_threshold() {
        // With a very high threshold, nothing should be an outlier
        let spans = vec![
            make_span("svc", "op", 100, vec![1]),
            make_span("svc", "op", 110, vec![2]),
            make_span("svc", "op", 200, vec![3]),
        ];
        let outliers = detect_outliers(&spans, 100.0);
        assert!(outliers.is_empty());

        // With a very low threshold, more things are outliers
        let outliers_low = detect_outliers(&spans, 0.1);
        assert!(
            !outliers_low.is_empty(),
            "low threshold should flag outliers"
        );
    }
}
