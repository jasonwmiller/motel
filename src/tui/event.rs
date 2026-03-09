use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use crate::store::StoreEvent;

use super::app::{App, InputMode, Tab, TraceView};

/// Possible outcomes from handling an event.
pub enum EventResult {
    /// Continue the main loop.
    Continue,
    /// Quit the application.
    Quit,
    /// Toggle pin on a trace by trace_id.
    TogglePin(Vec<u8>),
}

/// Poll for a crossterm terminal event with the given timeout.
/// Returns None if no event is available within the timeout.
pub fn poll_crossterm(timeout: Duration) -> std::io::Result<Option<Event>> {
    if event::poll(timeout)? {
        Ok(Some(event::read()?))
    } else {
        Ok(None)
    }
}

/// Handle a keyboard event, updating app state.
pub fn handle_key(app: &mut App, key: KeyEvent) -> EventResult {
    // Ctrl-C always quits
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return EventResult::Quit;
    }

    // Handle filter input mode
    if app.input_mode == InputMode::Filter {
        return handle_filter_key(app, key);
    }

    // When in timeline or diff view, intercept q and Esc to go back instead of quit
    let in_subview = matches!(app.trace_view, TraceView::Timeline(_) | TraceView::Diff);

    if in_subview {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                if app.timeline_detail_visible && matches!(app.trace_view, TraceView::Timeline(_)) {
                    app.timeline_detail_visible = false;
                    app.detail_scroll = 0;
                } else {
                    app.close_timeline();
                }
                return EventResult::Continue;
            }
            KeyCode::Enter => {
                if matches!(app.trace_view, TraceView::Timeline(_)) {
                    app.timeline_detail_visible = !app.timeline_detail_visible;
                    app.detail_scroll = 0;
                }
                return EventResult::Continue;
            }
            _ => {}
        }
    }

    match key.code {
        // Esc: if filter is active, clear it; otherwise quit
        KeyCode::Esc => {
            if !app.filter_text.is_empty() {
                app.clear_filter();
            } else {
                return EventResult::Quit;
            }
        }

        KeyCode::Char('q') => {
            return EventResult::Quit;
        }

        KeyCode::Char('/') => {
            app.input_mode = InputMode::Filter;
        }

        KeyCode::Tab => {
            if key.modifiers.contains(KeyModifiers::SHIFT) {
                app.prev_tab();
            } else {
                app.next_tab();
            }
        }
        KeyCode::BackTab => {
            app.prev_tab();
        }

        KeyCode::Char('1') => app.select_tab(Tab::Logs),
        KeyCode::Char('2') => app.select_tab(Tab::Traces),
        KeyCode::Char('3') => app.select_tab(Tab::Metrics),

        KeyCode::Char('f') => app.toggle_follow(),

        // Toggle metric graph mode (only on Metrics tab with enough data points)
        KeyCode::Char('g') => {
            if matches!(app.current_tab, Tab::Metrics) {
                app.metric_graph_mode = !app.metric_graph_mode;
                app.detail_scroll = 0;
            }
        }

        KeyCode::Up | KeyCode::Char('k') => app.move_up(),
        KeyCode::Down | KeyCode::Char('j') => app.move_down(),

        // PgUp/PgDn scroll the detail pane
        KeyCode::PageUp => {
            app.detail_scroll = app.detail_scroll.saturating_sub(10);
        }
        KeyCode::PageDown => {
            app.detail_scroll = app.detail_scroll.saturating_add(10);
        }

        KeyCode::Home => app.home(),
        KeyCode::End => app.end(),

        KeyCode::Enter => match app.current_tab {
            Tab::Traces => {
                app.open_trace();
            }
            Tab::Logs => {
                let idx = app.tab_states[Tab::Logs.index()].selected;
                if let Some(log) = app.log_rows.get(idx)
                    && !log.trace_id.is_empty()
                {
                    let trace_id = log.trace_id.clone();
                    app.navigate_to_trace(&trace_id);
                }
            }
            _ => {}
        },

        // Mark trace for diff
        KeyCode::Char('m') => {
            if matches!(app.current_tab, Tab::Traces) {
                app.mark_trace();
            }
        }

        // Diff marked trace with selected trace
        KeyCode::Char('d') => {
            if matches!(app.current_tab, Tab::Traces) {
                app.diff_traces();
            }
        }

        // Pin/unpin selected trace
        KeyCode::Char('p') => {
            if matches!(app.current_tab, Tab::Traces)
                && app.trace_view == TraceView::List
                && let Some(trace_id) = app.get_selected_trace_id()
            {
                return EventResult::TogglePin(trace_id);
            }
        }

        _ => {}
    }

    EventResult::Continue
}

fn handle_filter_key(app: &mut App, key: KeyEvent) -> EventResult {
    match key.code {
        KeyCode::Esc => {
            app.clear_filter();
        }
        KeyCode::Enter => {
            // Keep filter active, return to normal mode
            app.input_mode = InputMode::Normal;
        }
        KeyCode::Backspace => {
            if app.filter_cursor > 0 {
                app.filter_text.remove(app.filter_cursor - 1);
                app.filter_cursor -= 1;
                app.apply_filter();
                app.tab_states[app.current_tab.index()].selected = 0;
            }
        }
        KeyCode::Left => {
            if app.filter_cursor > 0 {
                app.filter_cursor -= 1;
            }
        }
        KeyCode::Right => {
            if app.filter_cursor < app.filter_text.len() {
                app.filter_cursor += 1;
            }
        }
        KeyCode::Char(c) => {
            app.filter_text.insert(app.filter_cursor, c);
            app.filter_cursor += 1;
            app.apply_filter();
            app.tab_states[app.current_tab.index()].selected = 0;
        }
        _ => {}
    }
    EventResult::Continue
}

/// Handle a store event by updating app state.
pub fn handle_store_event(app: &mut App, event: &StoreEvent) {
    app.handle_store_event(event);
}
