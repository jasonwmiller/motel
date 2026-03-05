use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Tabs, Wrap},
};

use crate::client::hex_encode;

use super::app::{App, Tab, format_any_value};

/// Render the entire UI.
pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tab bar
            Constraint::Min(5),    // main content
            Constraint::Length(1), // status bar
        ])
        .split(f.area());

    draw_tabs(f, app, chunks[0]);
    draw_main(f, app, chunks[1]);
    draw_status_bar(f, app, chunks[2]);

    if app.detail_open {
        draw_detail_overlay(f, app);
    }
}

fn draw_tabs(f: &mut Frame, app: &App, area: Rect) {
    let titles: Vec<Line> = Tab::all()
        .iter()
        .map(|t| {
            let count = match t {
                Tab::Traces => format!("{} spans ({} traces)", app.span_count, app.trace_count),
                Tab::Logs => format!("{}", app.log_count),
                Tab::Metrics => format!("{}", app.metric_count),
            };
            Line::from(format!(" {} ({}) ", t.label(), count))
        })
        .collect();

    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" motel - OpenTelemetry Viewer "),
        )
        .select(app.current_tab.index())
        .style(Style::default().fg(Color::Gray))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );

    f.render_widget(tabs, area);
}

fn draw_main(f: &mut Frame, app: &App, area: Rect) {
    match app.current_tab {
        Tab::Traces => draw_traces_table(f, app, area),
        Tab::Logs => draw_logs_table(f, app, area),
        Tab::Metrics => draw_metrics_table(f, app, area),
    }
}

fn draw_status_bar(f: &mut Frame, _app: &App, area: Rect) {
    let help = Line::from(vec![
        Span::styled(" Tab", Style::default().fg(Color::Yellow)),
        Span::raw(":switch  "),
        Span::styled("j/k", Style::default().fg(Color::Yellow)),
        Span::raw(":up/down  "),
        Span::styled("Enter", Style::default().fg(Color::Yellow)),
        Span::raw(":detail  "),
        Span::styled("PgUp/PgDn", Style::default().fg(Color::Yellow)),
        Span::raw(":page  "),
        Span::styled("Home/End", Style::default().fg(Color::Yellow)),
        Span::raw(":first/last  "),
        Span::styled("q/Esc", Style::default().fg(Color::Yellow)),
        Span::raw(":quit"),
    ]);
    let bar = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    f.render_widget(bar, area);
}

fn format_timestamp(nanos: u64) -> String {
    if nanos == 0 {
        return "-".to_string();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nsec = (nanos % 1_000_000_000) as u32;
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsec) {
        dt.format("%H:%M:%S%.3f").to_string()
    } else {
        "-".to_string()
    }
}

fn format_duration(ns: u64) -> String {
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
    let bg = if selected {
        Color::DarkGray
    } else if index % 2 == 0 {
        Color::Reset
    } else {
        Color::Rgb(30, 30, 30)
    };
    Style::default().bg(bg)
}

fn draw_traces_table(f: &mut Frame, app: &App, area: Rect) {
    let header_cells = ["Time", "Service", "Span Name", "Duration", "Trace ID"]
        .iter()
        .map(|h| {
            Cell::from(*h).style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
        });
    let header = Row::new(header_cells).height(1);

    let selected = app.tab_states[Tab::Traces.index()].selected;
    let visible_height = area.height.saturating_sub(3) as usize; // borders + header
    let offset = compute_scroll_offset(selected, visible_height, app.span_rows.len());

    let rows: Vec<Row> = app
        .span_rows
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, span)| {
            let tid = hex_encode(&span.trace_id);
            let tid_short = if tid.len() > 16 {
                format!("{}...", &tid[..16])
            } else {
                tid
            };
            let cells = vec![
                Cell::from(format_timestamp(span.time_nano)),
                Cell::from(span.service_name.clone()),
                Cell::from(span.span_name.clone()),
                Cell::from(format_duration(span.duration_ns)),
                Cell::from(tid_short),
            ];
            Row::new(cells).style(row_style(i, i == selected))
        })
        .collect();

    let widths = [
        Constraint::Length(14),
        Constraint::Length(20),
        Constraint::Fill(1),
        Constraint::Length(12),
        Constraint::Length(19),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Traces "));

    f.render_widget(table, area);
}

fn draw_logs_table(f: &mut Frame, app: &App, area: Rect) {
    let header_cells = ["Time", "Service", "Severity", "Body"].iter().map(|h| {
        Cell::from(*h).style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
    });
    let header = Row::new(header_cells).height(1);

    let selected = app.tab_states[Tab::Logs.index()].selected;
    let visible_height = area.height.saturating_sub(3) as usize;
    let offset = compute_scroll_offset(selected, visible_height, app.log_rows.len());

    let rows: Vec<Row> = app
        .log_rows
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, log)| {
            let sev_color = severity_color(&log.severity_text);
            let cells = vec![
                Cell::from(format_timestamp(log.time_nano)),
                Cell::from(log.service_name.clone()),
                Cell::from(log.severity_text.clone()).style(Style::default().fg(sev_color)),
                Cell::from(truncate(&log.body, 120)),
            ];
            Row::new(cells).style(row_style(i, i == selected))
        })
        .collect();

    let widths = [
        Constraint::Length(14),
        Constraint::Length(20),
        Constraint::Length(12),
        Constraint::Fill(1),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Logs "));

    f.render_widget(table, area);
}

fn draw_metrics_table(f: &mut Frame, app: &App, area: Rect) {
    let header_cells = ["Time", "Service", "Metric Name", "Type", "Value"]
        .iter()
        .map(|h| {
            Cell::from(*h).style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
        });
    let header = Row::new(header_cells).height(1);

    let selected = app.tab_states[Tab::Metrics.index()].selected;
    let visible_height = area.height.saturating_sub(3) as usize;
    let offset = compute_scroll_offset(selected, visible_height, app.metric_rows.len());

    let rows: Vec<Row> = app
        .metric_rows
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, met)| {
            let cells = vec![
                Cell::from(format_timestamp(met.time_nano)),
                Cell::from(met.service_name.clone()),
                Cell::from(met.metric_name.clone()),
                Cell::from(met.metric_type.clone()),
                Cell::from(truncate(&met.value, 40)),
            ];
            Row::new(cells).style(row_style(i, i == selected))
        })
        .collect();

    let widths = [
        Constraint::Length(14),
        Constraint::Length(20),
        Constraint::Fill(1),
        Constraint::Length(14),
        Constraint::Length(30),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Metrics "));

    f.render_widget(table, area);
}

/// Compute the scroll offset to keep the selected row visible.
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

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max.saturating_sub(3)])
    }
}

fn severity_color(sev: &str) -> Color {
    match sev {
        "TRACE" => Color::DarkGray,
        "DEBUG" => Color::Blue,
        "INFO" => Color::Green,
        "WARN" => Color::Yellow,
        "ERROR" => Color::Red,
        "FATAL" => Color::Magenta,
        _ => Color::White,
    }
}

// ---------------------------------------------------------------------------
// Detail overlay
// ---------------------------------------------------------------------------

fn draw_detail_overlay(f: &mut Frame, app: &App) {
    let area = f.area();
    // Use 80% of the screen, centered
    let width = (area.width * 4 / 5).max(40);
    let height = (area.height * 4 / 5).max(10);
    let x = (area.width.saturating_sub(width)) / 2;
    let y = (area.height.saturating_sub(height)) / 2;
    let overlay = Rect::new(x, y, width, height);

    f.render_widget(Clear, overlay);

    let lines = match app.current_tab {
        Tab::Traces => detail_span_lines(app),
        Tab::Logs => detail_log_lines(app),
        Tab::Metrics => detail_metric_lines(app),
    };

    let para = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Detail (Esc to close) ")
                .style(Style::default().bg(Color::Black)),
        )
        .wrap(Wrap { trim: false })
        .style(Style::default().bg(Color::Black));

    f.render_widget(para, overlay);
}

fn detail_span_lines(app: &App) -> Vec<Line<'static>> {
    let idx = app.tab_states[Tab::Traces.index()].selected;
    let Some(span) = app.span_rows.get(idx) else {
        return vec![Line::from("No span selected")];
    };

    let mut lines = vec![
        detail_line("Trace ID", &hex_encode(&span.trace_id)),
        detail_line("Span ID", &hex_encode(&span.span_id)),
        detail_line(
            "Parent Span ID",
            &if span.parent_span_id.is_empty() {
                "<root>".to_string()
            } else {
                hex_encode(&span.parent_span_id)
            },
        ),
        detail_line("Span Name", &span.span_name),
        detail_line("Service", &span.service_name),
        detail_line("Kind", &span_kind_str(span.kind)),
        detail_line("Start Time", &format_timestamp_full(span.time_nano)),
        detail_line("Duration", &format_duration(span.duration_ns)),
        detail_line("Status Code", &status_code_str(span.status_code)),
        detail_line("Status Message", &span.status_message),
        detail_line("Events", &span.events_count.to_string()),
        detail_line("Links", &span.links_count.to_string()),
        Line::from(""),
        section_header("Attributes"),
    ];
    for kv in &span.attributes {
        lines.push(detail_line(
            &format!("  {}", kv.key),
            &format_any_value(kv.value.as_ref()),
        ));
    }
    lines.push(Line::from(""));
    lines.push(section_header("Resource Attributes"));
    for kv in &span.resource_attributes {
        lines.push(detail_line(
            &format!("  {}", kv.key),
            &format_any_value(kv.value.as_ref()),
        ));
    }
    lines
}

fn detail_log_lines(app: &App) -> Vec<Line<'static>> {
    let idx = app.tab_states[Tab::Logs.index()].selected;
    let Some(log) = app.log_rows.get(idx) else {
        return vec![Line::from("No log selected")];
    };

    let mut lines = vec![
        detail_line("Time", &format_timestamp_full(log.time_nano)),
        detail_line("Service", &log.service_name),
        detail_line(
            "Severity",
            &format!("{} ({})", log.severity_text, log.severity_number),
        ),
        detail_line("Body", &log.body),
        detail_line(
            "Trace ID",
            &if log.trace_id.is_empty() {
                "-".to_string()
            } else {
                hex_encode(&log.trace_id)
            },
        ),
        detail_line(
            "Span ID",
            &if log.span_id.is_empty() {
                "-".to_string()
            } else {
                hex_encode(&log.span_id)
            },
        ),
        Line::from(""),
        section_header("Attributes"),
    ];
    for kv in &log.attributes {
        lines.push(detail_line(
            &format!("  {}", kv.key),
            &format_any_value(kv.value.as_ref()),
        ));
    }
    lines.push(Line::from(""));
    lines.push(section_header("Resource Attributes"));
    for kv in &log.resource_attributes {
        lines.push(detail_line(
            &format!("  {}", kv.key),
            &format_any_value(kv.value.as_ref()),
        ));
    }
    lines
}

fn detail_metric_lines(app: &App) -> Vec<Line<'static>> {
    let idx = app.tab_states[Tab::Metrics.index()].selected;
    let Some(met) = app.metric_rows.get(idx) else {
        return vec![Line::from("No metric selected")];
    };

    let mut lines = vec![
        detail_line("Metric Name", &met.metric_name),
        detail_line("Service", &met.service_name),
        detail_line("Type", &met.metric_type),
        detail_line("Value", &met.value),
        detail_line(
            "Unit",
            &if met.unit.is_empty() {
                "-".to_string()
            } else {
                met.unit.clone()
            },
        ),
        detail_line(
            "Description",
            &if met.description.is_empty() {
                "-".to_string()
            } else {
                met.description.clone()
            },
        ),
        detail_line("Time", &format_timestamp_full(met.time_nano)),
        Line::from(""),
        section_header("Data Point Attributes"),
    ];
    for kv in &met.attributes {
        lines.push(detail_line(
            &format!("  {}", kv.key),
            &format_any_value(kv.value.as_ref()),
        ));
    }
    lines.push(Line::from(""));
    lines.push(section_header("Resource Attributes"));
    for kv in &met.resource_attributes {
        lines.push(detail_line(
            &format!("  {}", kv.key),
            &format_any_value(kv.value.as_ref()),
        ));
    }
    lines
}

fn detail_line(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{}: ", label), Style::default().fg(Color::Cyan)),
        Span::raw(value.to_string()),
    ])
}

fn section_header(title: &str) -> Line<'static> {
    Line::from(Span::styled(
        title.to_string(),
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    ))
}

fn format_timestamp_full(nanos: u64) -> String {
    if nanos == 0 {
        return "-".to_string();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nsec = (nanos % 1_000_000_000) as u32;
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsec) {
        dt.format("%Y-%m-%d %H:%M:%S%.6f UTC").to_string()
    } else {
        "-".to_string()
    }
}

fn span_kind_str(kind: i32) -> String {
    match kind {
        0 => "UNSPECIFIED".to_string(),
        1 => "INTERNAL".to_string(),
        2 => "SERVER".to_string(),
        3 => "CLIENT".to_string(),
        4 => "PRODUCER".to_string(),
        5 => "CONSUMER".to_string(),
        _ => format!("UNKNOWN({})", kind),
    }
}

fn status_code_str(code: i32) -> String {
    match code {
        0 => "UNSET".to_string(),
        1 => "OK".to_string(),
        2 => "ERROR".to_string(),
        _ => format!("UNKNOWN({})", code),
    }
}
