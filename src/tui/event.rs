use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use crate::store::StoreEvent;

use super::app::{App, Tab, TraceView};

/// Possible outcomes from handling an event.
pub enum EventResult {
    /// Continue the main loop.
    Continue,
    /// Quit the application.
    Quit,
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

    // When in timeline view, intercept q and Esc to go back instead of quit
    let in_timeline = matches!(app.trace_view, TraceView::Timeline(_));

    if in_timeline {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                app.close_timeline();
                return EventResult::Continue;
            }
            KeyCode::Enter => {
                return EventResult::Continue;
            }
            _ => {}
        }
    }

    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => {
            return EventResult::Quit;
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

        KeyCode::Enter => {
            if matches!(app.current_tab, Tab::Traces) {
                app.open_trace();
            }
        }

        _ => {}
    }

    EventResult::Continue
}

/// Handle a store event by updating app state.
pub fn handle_store_event(app: &mut App, event: &StoreEvent) {
    app.handle_store_event(event);
}
