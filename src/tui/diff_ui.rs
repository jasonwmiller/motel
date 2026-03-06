use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table},
};

use crate::diff::SpanDiff;

use super::app::App;

// Colors matching the existing One Dark palette
const COLOR_SLOWER: Color = Color::Rgb(224, 108, 117); // soft red
const COLOR_FASTER: Color = Color::Rgb(152, 195, 121); // soft green
const COLOR_ADDED: Color = Color::Rgb(97, 175, 239); // soft blue
const COLOR_REMOVED: Color = Color::Rgb(100, 100, 100); // gray
const COLOR_HEADER: Color = Color::Rgb(86, 182, 194); // teal
const COLOR_NEUTRAL: Color = Color::Rgb(171, 178, 191); // light gray

/// Draw the diff comparison view.
pub fn draw_diff(f: &mut Frame, app: &App, area: Rect) {
    let diff = match &app.diff_result {
        Some(d) => d,
        None => {
            let block = Block::default()
                .borders(Borders::ALL)
                .title(" Diff (Esc:back) ");
            f.render_widget(block, area);
            return;
        }
    };

    let selected = app.diff_selected;
    let visible_height = area.height.saturating_sub(4) as usize; // header + borders + summary
    let total = diff.span_diffs.len();
    let offset = compute_scroll_offset(selected, visible_height, total);

    // Build summary for the title
    let dur_a = format_duration_ms(diff.total_duration_a);
    let dur_b = format_duration_ms(diff.total_duration_b);
    let total_delta = diff.total_duration_b as i64 - diff.total_duration_a as i64;
    let total_pct = if diff.total_duration_a == 0 {
        0.0
    } else {
        (total_delta as f64 / diff.total_duration_a as f64) * 100.0
    };
    let sign = if total_pct >= 0.0 { "+" } else { "" };
    let title = format!(
        " Diff: {} -> {} ({}{:.1}%) (Esc:back) ",
        dur_a, dur_b, sign, total_pct
    );

    // Header row
    let header = Row::new(vec![
        Cell::from("Span Name").style(
            Style::default()
                .fg(COLOR_HEADER)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Service").style(
            Style::default()
                .fg(COLOR_HEADER)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("A Duration").style(
            Style::default()
                .fg(COLOR_HEADER)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("B Duration").style(
            Style::default()
                .fg(COLOR_HEADER)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("Change").style(
            Style::default()
                .fg(COLOR_HEADER)
                .add_modifier(Modifier::BOLD),
        ),
    ])
    .height(1);

    let threshold = 20.0_f64; // default threshold for color coding

    let rows: Vec<Row> = diff
        .span_diffs
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, sd)| {
            let is_selected = i == selected;
            let selection_marker = if is_selected { "\u{25b6} " } else { "" };

            match sd {
                SpanDiff::Matched {
                    span_a,
                    span_b,
                    duration_pct_change,
                    ..
                } => {
                    let change_color = if duration_pct_change.abs() < threshold {
                        COLOR_NEUTRAL
                    } else if *duration_pct_change > 0.0 {
                        COLOR_SLOWER
                    } else {
                        COLOR_FASTER
                    };
                    let s = if *duration_pct_change >= 0.0 { "+" } else { "" };
                    let change_str = format!("{}{:.1}%", s, duration_pct_change);

                    let cells = vec![
                        Cell::from(format!("{}{}", selection_marker, span_a.span_name)),
                        Cell::from(span_a.service_name.clone()),
                        Cell::from(format_duration_ms(span_a.duration_ns)),
                        Cell::from(format_duration_ms(span_b.duration_ns)),
                        Cell::from(change_str).style(Style::default().fg(change_color)),
                    ];
                    Row::new(cells).style(row_style(i, is_selected))
                }
                SpanDiff::OnlyInA(span) => {
                    let cells = vec![
                        Cell::from(format!("{}- {}", selection_marker, span.span_name))
                            .style(Style::default().fg(COLOR_REMOVED)),
                        Cell::from(span.service_name.clone())
                            .style(Style::default().fg(COLOR_REMOVED)),
                        Cell::from(format_duration_ms(span.duration_ns))
                            .style(Style::default().fg(COLOR_REMOVED)),
                        Cell::from("-").style(Style::default().fg(COLOR_REMOVED)),
                        Cell::from("removed").style(Style::default().fg(COLOR_REMOVED)),
                    ];
                    Row::new(cells).style(row_style(i, is_selected))
                }
                SpanDiff::OnlyInB(span) => {
                    let cells = vec![
                        Cell::from(format!("{}+ {}", selection_marker, span.span_name))
                            .style(Style::default().fg(COLOR_ADDED)),
                        Cell::from(span.service_name.clone())
                            .style(Style::default().fg(COLOR_ADDED)),
                        Cell::from("-").style(Style::default().fg(COLOR_ADDED)),
                        Cell::from(format_duration_ms(span.duration_ns))
                            .style(Style::default().fg(COLOR_ADDED)),
                        Cell::from("+new").style(Style::default().fg(COLOR_ADDED)),
                    ];
                    Row::new(cells).style(row_style(i, is_selected))
                }
            }
        })
        .collect();

    let widths = [
        Constraint::Fill(1),
        Constraint::Length(20),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(10),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));

    f.render_widget(table, area);
}

fn format_duration_ms(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.3}s", ns as f64 / 1_000_000_000.0)
    }
}

fn row_style(index: usize, selected: bool) -> Style {
    if selected {
        Style::default()
            .bg(Color::Rgb(50, 55, 70))
            .fg(Color::Rgb(220, 223, 228))
    } else if index.is_multiple_of(2) {
        Style::default()
    } else {
        Style::default().bg(Color::Rgb(30, 33, 39))
    }
}

fn compute_scroll_offset(selected: usize, visible: usize, total: usize) -> usize {
    if total == 0 || visible == 0 {
        return 0;
    }
    if selected < visible / 2 {
        0
    } else if selected + visible / 2 >= total {
        total.saturating_sub(visible)
    } else {
        selected.saturating_sub(visible / 2)
    }
}
