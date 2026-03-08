use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs, Wrap},
};

use crate::client::hex_encode;

use crate::otel::common::v1::KeyValue;

use super::app::{App, Tab, TraceView, format_any_value};

// ---------------------------------------------------------------------------
// Main draw entry point
// ---------------------------------------------------------------------------

/// Render the entire UI.
pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tab bar
            Constraint::Min(5),    // main content
            Constraint::Length(1), // status bar
        ])
        .split(f.area());

    draw_tabs(f, app, chunks[0]);

    // Pre-populate service colors so we can use immutable access during rendering.
    ensure_service_colors(app);

    draw_main(f, app, chunks[1]);
    draw_status_bar(f, app, chunks[2]);
}

/// Pre-populate service_colors for all known services so that draw functions
/// can use `app.service_colors.get()` (immutable) instead of `app.service_color()` (mutable).
fn ensure_service_colors(app: &mut App) {
    let mut services: Vec<String> = Vec::new();
    for log in &app.log_rows {
        services.push(log.service_name.clone());
    }
    for group in &app.trace_groups {
        services.push(group.service_name.clone());
        for span in &group.spans {
            services.push(span.service_name.clone());
        }
    }
    for node in &app.timeline_nodes {
        services.push(node.span.service_name.clone());
        services.push(node.span.span_name.clone());
    }
    for met in &app.aggregated_metrics {
        services.push(met.service_name.clone());
    }
    for svc in services {
        app.service_color(&svc);
    }
}

/// Look up a previously-registered service color, falling back to White.
fn get_service_color(app: &App, service: &str) -> Color {
    app.service_colors
        .get(service)
        .copied()
        .unwrap_or(Color::White)
}

// ---------------------------------------------------------------------------
// Source server extraction
// ---------------------------------------------------------------------------

/// Extract the `motel.source` resource attribute value, if present.
fn extract_source(resource_attrs: &[KeyValue]) -> Option<String> {
    for kv in resource_attrs {
        if kv.key == "motel.source" {
            return Some(format_any_value(kv.value.as_ref()));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tab bar
// ---------------------------------------------------------------------------

fn draw_tabs(f: &mut Frame, app: &App, area: Rect) {
    let titles: Vec<Line> = Tab::all()
        .iter()
        .map(|t| {
            let count = match t {
                Tab::Traces => {
                    format!("{} spans ({} traces)", app.span_count, app.trace_count)
                }
                Tab::Logs => format!("{}", app.log_count),
                Tab::Metrics => format!("{}", app.metric_count),
            };
            Line::from(format!("{}:{}({})", t.number(), t.label(), count))
        })
        .collect();

    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" OTLP Viewer "),
        )
        .select(app.current_tab.index())
        .style(Style::default().fg(Color::Gray))
        .highlight_style(
            Style::default()
                .fg(Color::Rgb(229, 192, 123))
                .add_modifier(Modifier::BOLD),
        )
        .divider(" | ");

    f.render_widget(tabs, area);
}

// ---------------------------------------------------------------------------
// Status bar
// ---------------------------------------------------------------------------

fn draw_status_bar(f: &mut Frame, app: &App, area: Rect) {
    let follow_span = if app.follow_mode {
        Span::styled("[FOLLOW]", Style::default().fg(Color::Rgb(152, 195, 121)))
    } else {
        Span::styled("[PAUSED]", Style::default().fg(Color::Rgb(229, 192, 123)))
    };

    let key_color = Color::Rgb(209, 154, 102);
    let mut help = vec![
        Span::styled(" Tab", Style::default().fg(key_color)),
        Span::raw(":switch  "),
        Span::styled("j/k", Style::default().fg(key_color)),
        Span::raw(":nav  "),
        Span::styled("Enter", Style::default().fg(key_color)),
        Span::raw(":select  "),
        Span::styled("PgUp/Dn", Style::default().fg(key_color)),
        Span::raw(":scroll  "),
        Span::styled("f", Style::default().fg(key_color)),
        Span::raw(":follow  "),
    ];
    if matches!(app.current_tab, Tab::Metrics) {
        help.push(Span::styled("g", Style::default().fg(key_color)));
        help.push(Span::raw(":graph  "));
    }
    if matches!(app.current_tab, Tab::Traces) && app.trace_view == TraceView::List {
        help.push(Span::styled("m", Style::default().fg(key_color)));
        help.push(Span::raw(":mark  "));
        help.push(Span::styled("d", Style::default().fg(key_color)));
        help.push(Span::raw(":diff  "));
        help.push(Span::styled("p", Style::default().fg(key_color)));
        help.push(Span::raw(":pin  "));
    }
    help.push(Span::styled("q", Style::default().fg(key_color)));
    help.push(Span::raw(":quit"));

    let bar_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Length(10)])
        .split(area);

    let left = Paragraph::new(Line::from(help)).style(Style::default().fg(Color::DarkGray));
    let right =
        Paragraph::new(Line::from(follow_span)).alignment(ratatui::layout::Alignment::Right);

    f.render_widget(left, bar_layout[0]);
    f.render_widget(right, bar_layout[1]);
}

// ---------------------------------------------------------------------------
// Main content dispatch
// ---------------------------------------------------------------------------

fn draw_main(f: &mut Frame, app: &App, area: Rect) {
    match app.current_tab {
        Tab::Logs => draw_logs(f, app, area),
        Tab::Traces => draw_traces(f, app, area),
        Tab::Metrics => draw_metrics(f, app, area),
    }
}

// ---------------------------------------------------------------------------
// Logs tab: master-detail 60/40
// ---------------------------------------------------------------------------

fn draw_logs(f: &mut Frame, app: &App, area: Rect) {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    draw_logs_table(f, app, split[0]);
    draw_log_detail(f, app, split[1]);
}

fn draw_logs_table(f: &mut Frame, app: &App, area: Rect) {
    let multi = app.multi_server;
    let mut header_names: Vec<&str> = vec!["Time", "Service", "Severity", "Body"];
    if multi {
        header_names.insert(1, "Source");
    }
    let header_cells = header_names.iter().map(|h| {
        Cell::from(*h).style(
            Style::default()
                .fg(Color::Rgb(86, 182, 194))
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
            let is_selected = i == selected;
            let sev_color = severity_color(&log.severity_text);
            let time_str = if is_selected {
                format!("\u{25b6} {}", format_timestamp(log.time_nano))
            } else {
                format_timestamp(log.time_nano)
            };
            let svc_color = get_service_color(app, &log.service_name);
            let mut cells = vec![Cell::from(time_str)];
            if multi {
                let source = extract_source(&log.resource_attributes).unwrap_or_default();
                cells.push(
                    Cell::from(truncate(&source, 20))
                        .style(Style::default().fg(Color::Rgb(190, 190, 190))),
                );
            }
            cells.extend([
                Cell::from(log.service_name.clone()).style(Style::default().fg(svc_color)),
                Cell::from(log.severity_text.clone()).style(Style::default().fg(sev_color)),
                Cell::from(truncate(&log.body, 120)),
            ]);
            Row::new(cells).style(row_style(i, is_selected))
        })
        .collect();

    let widths: Vec<Constraint> = if multi {
        vec![
            Constraint::Length(16),
            Constraint::Length(22),
            Constraint::Length(20),
            Constraint::Length(12),
            Constraint::Fill(1),
        ]
    } else {
        vec![
            Constraint::Length(16),
            Constraint::Length(20),
            Constraint::Length(12),
            Constraint::Fill(1),
        ]
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Logs "));

    f.render_widget(table, area);
}

fn draw_log_detail(f: &mut Frame, app: &App, area: Rect) {
    let idx = app.tab_states[Tab::Logs.index()].selected;
    let lines = if let Some(log) = app.log_rows.get(idx) {
        let mut l = vec![
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
            detail_line("Scope", &log.scope_name),
            Line::from(""),
            section_header("Attributes"),
        ];
        for kv in &log.attributes {
            l.push(detail_line(
                &format!("  {}", kv.key),
                &format_any_value(kv.value.as_ref()),
            ));
        }
        l.push(Line::from(""));
        l.push(section_header("Resource Attributes"));
        for kv in &log.resource_attributes {
            l.push(detail_line(
                &format!("  {}", kv.key),
                &format_any_value(kv.value.as_ref()),
            ));
        }
        l
    } else {
        vec![Line::from("No log selected")]
    };

    let para = Paragraph::new(Text::from(lines))
        .block(Block::default().borders(Borders::ALL).title(" Detail "))
        .wrap(Wrap { trim: false })
        .scroll((app.detail_scroll, 0));

    f.render_widget(para, area);
}

// ---------------------------------------------------------------------------
// Traces tab
// ---------------------------------------------------------------------------

fn draw_traces(f: &mut Frame, app: &App, area: Rect) {
    match &app.trace_view {
        TraceView::List => draw_traces_list(f, app, area),
        TraceView::Timeline(_) => draw_traces_timeline(f, app, area),
        TraceView::Diff => super::diff_ui::draw_diff(f, app, area),
    }
}

fn draw_traces_list(f: &mut Frame, app: &App, area: Rect) {
    let multi = app.multi_server;
    let mut header_names: Vec<&str> = vec!["Trace ID", "Service", "Root Span", "Spans", "Duration"];
    if multi {
        header_names.insert(1, "Source");
    }
    let header_cells = header_names.iter().map(|h| {
        Cell::from(*h).style(
            Style::default()
                .fg(Color::Rgb(86, 182, 194))
                .add_modifier(Modifier::BOLD),
        )
    });
    let header = Row::new(header_cells).height(1);

    let selected = app.tab_states[Tab::Traces.index()].selected;
    let visible_height = area.height.saturating_sub(3) as usize;
    let offset = compute_scroll_offset(selected, visible_height, app.trace_groups.len());

    let rows: Vec<Row> = app
        .trace_groups
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, group)| {
            let is_selected = i == selected;
            let tid = hex_encode(&group.trace_id);
            let tid_short = if tid.len() > 8 { &tid[..8] } else { &tid };
            let is_marked = app
                .marked_trace_id
                .as_ref()
                .is_some_and(|m| *m == group.trace_id);
            let mark_indicator = if is_marked { "*" } else { "" };
            let pin_indicator = if group.pinned { "^ " } else { "" };
            let tid_display = if is_selected {
                format!("\u{25b6}{} {}{}", mark_indicator, pin_indicator, tid_short)
            } else {
                format!("{}{}{}", mark_indicator, pin_indicator, tid_short)
            };
            let svc_color = get_service_color(app, &group.service_name);
            let mut cells = vec![Cell::from(tid_display)];
            if multi {
                // Extract source from the root span's resource attributes
                let source = group
                    .spans
                    .first()
                    .and_then(|s| extract_source(&s.resource_attributes))
                    .unwrap_or_default();
                cells.push(
                    Cell::from(truncate(&source, 20))
                        .style(Style::default().fg(Color::Rgb(190, 190, 190))),
                );
            }
            cells.extend([
                Cell::from(group.service_name.clone()).style(Style::default().fg(svc_color)),
                Cell::from(truncate(&group.root_span_name, 40)),
                Cell::from(group.span_count.to_string()),
                Cell::from(format_duration(group.duration_ns)),
            ]);
            Row::new(cells).style(row_style(i, is_selected))
        })
        .collect();

    let widths: Vec<Constraint> = if multi {
        vec![
            Constraint::Length(12),
            Constraint::Length(22),
            Constraint::Length(20),
            Constraint::Fill(1),
            Constraint::Length(8),
            Constraint::Length(12),
        ]
    } else {
        vec![
            Constraint::Length(12),
            Constraint::Length(20),
            Constraint::Fill(1),
            Constraint::Length(8),
            Constraint::Length(12),
        ]
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Traces "));

    f.render_widget(table, area);
}

// ---------------------------------------------------------------------------
// Traces timeline (waterfall) view
// ---------------------------------------------------------------------------

fn draw_traces_timeline(f: &mut Frame, app: &App, area: Rect) {
    let wide = area.width > 120;

    if wide {
        let split = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(area);
        draw_timeline_waterfall(f, app, split[0]);
        draw_timeline_detail(f, app, split[1]);
    } else {
        draw_timeline_waterfall(f, app, area);
    }
}

fn draw_timeline_waterfall(f: &mut Frame, app: &App, area: Rect) {
    let nodes = &app.timeline_nodes;
    let selected = app.timeline_selected;

    if nodes.is_empty() {
        let empty = Paragraph::new("No spans in this trace").block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Timeline (Esc:back) "),
        );
        f.render_widget(empty, area);
        return;
    }

    // Compute trace time range
    let min_time = nodes.iter().map(|n| n.span.time_nano).min().unwrap_or(0);
    let max_time = nodes
        .iter()
        .map(|n| n.span.time_nano + n.span.duration_ns)
        .max()
        .unwrap_or(0);
    let trace_duration = max_time.saturating_sub(min_time).max(1);

    let inner_width = area.width.saturating_sub(2) as usize;
    let visible_height = area.height.saturating_sub(3) as usize;
    let offset = compute_scroll_offset(selected, visible_height, nodes.len());

    let name_width = inner_width / 2;
    let bar_width = inner_width.saturating_sub(name_width + 1);

    let rows: Vec<Row> = nodes
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, node)| {
            let is_selected = i == selected;

            let indent = "  ".repeat(node.depth);
            let prefix_char = if node.depth == 0 {
                "\u{2500}\u{2500} "
            } else {
                "\u{251c}\u{2500} "
            };
            let selection_marker = if is_selected { "\u{25b6} " } else { "" };
            let name_str = format!(
                "{}{}{}{}",
                selection_marker, indent, prefix_char, node.span.span_name
            );
            let name_truncated = truncate(&name_str, name_width);

            let span_start = node.span.time_nano.saturating_sub(min_time);
            let span_end = span_start + node.span.duration_ns;

            let bar_start =
                ((span_start as f64 / trace_duration as f64) * bar_width as f64) as usize;
            let bar_end = ((span_end as f64 / trace_duration as f64) * bar_width as f64) as usize;
            let bar_len = bar_end.saturating_sub(bar_start).max(1).min(bar_width);
            let bar_start_clamped = bar_start.min(bar_width.saturating_sub(1));

            let svc_color = get_service_color(app, &node.span.span_name);

            let mut bar_string = " ".repeat(bar_start_clamped);
            bar_string.push_str(&"\u{2588}".repeat(bar_len));

            let padded_name = format!("{:<width$}", name_truncated, width = name_width);

            let cells = vec![
                Cell::from(padded_name),
                Cell::from(Span::styled(bar_string, Style::default().fg(svc_color))),
            ];
            Row::new(cells).style(row_style(i, is_selected))
        })
        .collect();

    let widths = [Constraint::Length(name_width as u16), Constraint::Fill(1)];

    let header = Row::new(vec![
        Cell::from("Span").style(
            Style::default()
                .fg(Color::Rgb(86, 182, 194))
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from(format!("Timeline ({})", format_duration(trace_duration))).style(
            Style::default()
                .fg(Color::Rgb(86, 182, 194))
                .add_modifier(Modifier::BOLD),
        ),
    ])
    .height(1);

    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Timeline (Esc:back) "),
    );

    f.render_widget(table, area);
}

fn draw_timeline_detail(f: &mut Frame, app: &App, area: Rect) {
    let selected = app.timeline_selected;
    let lines = if let Some(node) = app.timeline_nodes.get(selected) {
        span_detail_lines(&node.span)
    } else {
        vec![Line::from("No span selected")]
    };

    let para = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Span Detail "),
        )
        .wrap(Wrap { trim: false })
        .scroll((app.detail_scroll, 0));

    f.render_widget(para, area);
}

// ---------------------------------------------------------------------------
// Metrics tab: master-detail 60/40
// ---------------------------------------------------------------------------

fn draw_metrics(f: &mut Frame, app: &App, area: Rect) {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    draw_metrics_table(f, app, split[0]);
    draw_metric_detail(f, app, split[1]);
}

fn draw_metrics_table(f: &mut Frame, app: &App, area: Rect) {
    let multi = app.multi_server;
    let mut header_names: Vec<&str> = vec!["Metric Name", "Service", "Type", "Unit", "Value"];
    if multi {
        header_names.insert(1, "Source");
    }
    let header_cells = header_names.iter().map(|h| {
        Cell::from(*h).style(
            Style::default()
                .fg(Color::Rgb(86, 182, 194))
                .add_modifier(Modifier::BOLD),
        )
    });
    let header = Row::new(header_cells).height(1);

    let selected = app.tab_states[Tab::Metrics.index()].selected;
    let visible_height = area.height.saturating_sub(3) as usize;
    let offset = compute_scroll_offset(selected, visible_height, app.aggregated_metrics.len());

    let rows: Vec<Row> = app
        .aggregated_metrics
        .iter()
        .enumerate()
        .skip(offset)
        .take(visible_height)
        .map(|(i, met)| {
            let is_selected = i == selected;
            let name_str = if is_selected {
                format!("\u{25b6} {}", met.metric_name)
            } else {
                met.metric_name.clone()
            };
            let svc_color = get_service_color(app, &met.service_name);
            let mut cells = vec![Cell::from(truncate(&name_str, 40))];
            if multi {
                let source = extract_source(&met.resource_attributes).unwrap_or_default();
                cells.push(
                    Cell::from(truncate(&source, 20))
                        .style(Style::default().fg(Color::Rgb(190, 190, 190))),
                );
            }
            cells.extend([
                Cell::from(met.service_name.clone()).style(Style::default().fg(svc_color)),
                Cell::from(met.metric_type.clone()),
                Cell::from(if met.unit.is_empty() {
                    "-".to_string()
                } else {
                    met.unit.clone()
                }),
                Cell::from(truncate(&met.display_value(), 20)),
            ]);
            Row::new(cells).style(row_style(i, is_selected))
        })
        .collect();

    let widths: Vec<Constraint> = if multi {
        vec![
            Constraint::Fill(1),
            Constraint::Length(22),
            Constraint::Length(20),
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(20),
        ]
    } else {
        vec![
            Constraint::Fill(1),
            Constraint::Length(20),
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(20),
        ]
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Metrics "));

    f.render_widget(table, area);
}

fn draw_metric_detail(f: &mut Frame, app: &App, area: Rect) {
    let idx = app.tab_states[Tab::Metrics.index()].selected;
    let Some(met) = app.aggregated_metrics.get(idx) else {
        let para = Paragraph::new("No metric selected")
            .block(Block::default().borders(Borders::ALL).title(" Detail "));
        f.render_widget(para, area);
        return;
    };

    let has_graph_data =
        met.data_points.len() >= 5 && parse_metric_values(&met.data_points).is_some();

    if app.metric_graph_mode && has_graph_data {
        draw_metric_graph(f, app, met, area);
    } else {
        draw_metric_detail_text(f, app, met, has_graph_data, area);
    }
}

fn draw_metric_detail_text(
    f: &mut Frame,
    app: &App,
    met: &super::app::AggregatedMetric,
    has_graph_data: bool,
    area: Rect,
) {
    let mut l = vec![
        detail_line("Metric Name", &met.metric_name),
        detail_line("Service", &met.service_name),
        detail_line("Type", &met.metric_type),
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
        detail_line("Data Points", &met.data_points.len().to_string()),
    ];
    if has_graph_data {
        l.push(Line::from(Span::styled(
            "  Press 'g' for graph view".to_string(),
            Style::default().fg(Color::Rgb(100, 100, 100)),
        )));
    }
    l.push(Line::from(""));
    l.push(section_header("Recent Data Points"));
    for dp in &met.data_points {
        l.push(detail_line(
            &format!("  {}", format_timestamp(dp.time_nano)),
            &dp.value,
        ));
        for kv in &dp.attributes {
            l.push(detail_line(
                &format!("    {}", kv.key),
                &format_any_value(kv.value.as_ref()),
            ));
        }
    }

    let para = Paragraph::new(Text::from(l))
        .block(Block::default().borders(Borders::ALL).title(" Detail "))
        .wrap(Wrap { trim: false })
        .scroll((app.detail_scroll, 0));

    f.render_widget(para, area);
}

fn draw_metric_graph(f: &mut Frame, _app: &App, met: &super::app::AggregatedMetric, area: Rect) {
    let inner_area = area.inner(ratatui::layout::Margin {
        vertical: 1,
        horizontal: 1,
    });

    // Parse values and sort by time ascending
    let values = match parse_metric_values(&met.data_points) {
        Some(v) => v,
        None => {
            let para = Paragraph::new("Cannot parse numeric values for graph")
                .block(Block::default().borders(Borders::ALL).title(" Graph "));
            f.render_widget(para, area);
            return;
        }
    };

    if values.is_empty() || inner_area.width < 4 || inner_area.height < 4 {
        let para = Paragraph::new("Not enough space for graph")
            .block(Block::default().borders(Borders::ALL).title(" Graph "));
        f.render_widget(para, area);
        return;
    }

    let min_val = values.iter().map(|v| v.1).fold(f64::INFINITY, f64::min);
    let max_val = values.iter().map(|v| v.1).fold(f64::NEG_INFINITY, f64::max);
    let range = (max_val - min_val).max(0.001);

    // Build header info
    let title = format!(
        " {} ({}) - press 'g' for detail ",
        met.metric_name,
        if met.unit.is_empty() {
            &met.metric_type
        } else {
            &met.unit
        }
    );

    let block = Block::default().borders(Borders::ALL).title(title);
    f.render_widget(block, area);

    // Graph area inside borders
    let graph_area = Rect::new(
        inner_area.x + 1,
        inner_area.y + 1,
        inner_area.width.saturating_sub(2),
        inner_area.height.saturating_sub(3),
    );

    if graph_area.width < 2 || graph_area.height < 2 {
        return;
    }

    let chart_height = graph_area.height as usize;
    let chart_width = graph_area.width as usize;

    // Sample or interpolate values to fit chart width
    let sampled: Vec<f64> = if values.len() <= chart_width {
        values.iter().map(|v| v.1).collect()
    } else {
        // Downsample: pick evenly spaced points
        (0..chart_width)
            .map(|i| {
                let idx = i * (values.len() - 1) / (chart_width - 1);
                values[idx].1
            })
            .collect()
    };

    // Bar characters for sub-cell resolution
    let bar_chars = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    // Draw columns
    for (col, &val) in sampled.iter().enumerate() {
        if col >= chart_width {
            break;
        }
        let normalized = (val - min_val) / range;
        let total_eighths = (normalized * (chart_height * 8) as f64) as usize;
        let full_blocks = total_eighths / 8;
        let remainder = total_eighths % 8;

        for row in 0..chart_height {
            let y = graph_area.y + (chart_height - 1 - row) as u16;
            let x = graph_area.x + col as u16;

            let ch = if row < full_blocks {
                '█'
            } else if row == full_blocks && remainder > 0 {
                bar_chars[remainder - 1]
            } else {
                ' '
            };

            let buf = f.buffer_mut();
            if x < buf.area.right() && y < buf.area.bottom() {
                buf[(x, y)].set_char(ch).set_fg(Color::Rgb(97, 175, 239));
            }
        }
    }

    // Draw axis labels
    let buf = f.buffer_mut();
    let max_label = format_compact_value(max_val);
    let min_label = format_compact_value(min_val);

    // Top label (max)
    let label_y = inner_area.y;
    let label_x = inner_area.x;
    for (i, ch) in max_label.chars().enumerate() {
        let x = label_x + i as u16;
        if x < buf.area.right() && label_y < buf.area.bottom() {
            buf[(x, label_y)]
                .set_char(ch)
                .set_fg(Color::Rgb(100, 100, 100));
        }
    }

    // Bottom label (min)
    let bottom_y = inner_area.y + inner_area.height.saturating_sub(1);
    for (i, ch) in min_label.chars().enumerate() {
        let x = label_x + i as u16;
        if x < buf.area.right() && bottom_y < buf.area.bottom() {
            buf[(x, bottom_y)]
                .set_char(ch)
                .set_fg(Color::Rgb(100, 100, 100));
        }
    }
}

/// Parse metric data point values into (time_nano, f64) pairs, sorted by time ascending.
fn parse_metric_values(data_points: &[super::app::MetricDataPoint]) -> Option<Vec<(u64, f64)>> {
    let mut values: Vec<(u64, f64)> = Vec::new();
    for dp in data_points {
        let v = dp.value.parse::<f64>().ok()?;
        values.push((dp.time_nano, v));
    }
    values.sort_by_key(|v| v.0);
    Some(values)
}

fn format_compact_value(v: f64) -> String {
    let abs = v.abs();
    if abs >= 1_000_000_000.0 {
        format!("{:.1}G", v / 1_000_000_000.0)
    } else if abs >= 1_000_000.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else if abs >= 1_000.0 {
        format!("{:.1}K", v / 1_000.0)
    } else if abs >= 1.0 || abs == 0.0 {
        format!("{:.1}", v)
    } else {
        format!("{:.4}", v)
    }
}

// ---------------------------------------------------------------------------
// Shared span detail lines (used in timeline detail)
// ---------------------------------------------------------------------------

fn span_detail_lines(span: &super::app::SpanRow) -> Vec<Line<'static>> {
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

// ---------------------------------------------------------------------------
// Shared utility functions
// ---------------------------------------------------------------------------

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
        let end = max.saturating_sub(3);
        let boundary = s
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|&i| i <= end)
            .last()
            .unwrap_or(0);
        format!("{}...", &s[..boundary])
    }
}

fn severity_color(sev: &str) -> Color {
    match sev {
        "TRACE" => Color::Rgb(100, 100, 100),
        "DEBUG" => Color::Rgb(97, 175, 239),
        "INFO" => Color::Rgb(152, 195, 121),
        "WARN" => Color::Rgb(229, 192, 123),
        "ERROR" => Color::Rgb(224, 108, 117),
        "FATAL" => Color::Rgb(198, 120, 221),
        _ => Color::Rgb(171, 178, 191),
    }
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
    if selected {
        Style::default()
            .bg(Color::Rgb(50, 55, 70))
            .fg(Color::Rgb(220, 223, 228))
    } else if index % 2 == 0 {
        Style::default()
    } else {
        Style::default().bg(Color::Rgb(30, 33, 39))
    }
}

fn detail_line(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{}: ", label),
            Style::default().fg(Color::Rgb(86, 182, 194)),
        ),
        Span::raw(value.to_string()),
    ])
}

fn section_header(title: &str) -> Line<'static> {
    Line::from(Span::styled(
        title.to_string(),
        Style::default()
            .fg(Color::Rgb(229, 192, 123))
            .add_modifier(Modifier::BOLD),
    ))
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
