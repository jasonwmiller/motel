use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use crate::store::StoreEvent;

use super::app::{App, Tab};

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

    match key.code {
        KeyCode::Char('q') => {
            if app.detail_open {
                app.detail_open = false;
            } else {
                return EventResult::Quit;
            }
        }
        KeyCode::Esc => {
            if app.detail_open {
                app.detail_open = false;
            } else {
                return EventResult::Quit;
            }
        }
        KeyCode::Tab => {
            if key.modifiers.contains(KeyModifiers::SHIFT) {
                app.prev_tab();
            } else {
                app.next_tab();
            }
            app.detail_open = false;
        }
        KeyCode::BackTab => {
            app.prev_tab();
            app.detail_open = false;
        }
        KeyCode::Char('1') => {
            app.select_tab(Tab::Traces);
            app.detail_open = false;
        }
        KeyCode::Char('2') => {
            app.select_tab(Tab::Logs);
            app.detail_open = false;
        }
        KeyCode::Char('3') => {
            app.select_tab(Tab::Metrics);
            app.detail_open = false;
        }
        KeyCode::Up | KeyCode::Char('k') => app.move_up(),
        KeyCode::Down | KeyCode::Char('j') => app.move_down(),
        KeyCode::PageUp => app.page_up(20),
        KeyCode::PageDown => app.page_down(20),
        KeyCode::Home => app.home(),
        KeyCode::End => app.end(),
        KeyCode::Enter => app.toggle_detail(),
        _ => {}
    }

    EventResult::Continue
}

/// Handle a store event by marking the appropriate tab dirty.
pub fn handle_store_event(app: &mut App, event: &StoreEvent) {
    app.handle_store_event(event);
}
