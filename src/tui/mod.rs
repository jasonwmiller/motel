pub mod app;
pub mod event;
pub mod ui;

use std::io;
use std::time::Duration;

use crossterm::{
    event::Event,
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::sync::broadcast;

use crate::store::{SharedStore, StoreEvent};

use self::app::App;
use self::event::{EventResult, handle_key, handle_store_event, poll_crossterm};

/// Run the TUI, reading from the shared store and listening for events.
pub async fn run(
    store: SharedStore,
    mut event_rx: broadcast::Receiver<StoreEvent>,
) -> anyhow::Result<()> {
    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, &store, &mut event_rx).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    store: &SharedStore,
    event_rx: &mut broadcast::Receiver<StoreEvent>,
) -> anyhow::Result<()> {
    let mut app = App::new();

    // Initial load
    app.refresh_from_store(store).await;
    terminal.draw(|f| ui::draw(f, &mut app))?;

    loop {
        // Drain any pending store events (non-blocking)
        loop {
            match event_rx.try_recv() {
                Ok(ev) => handle_store_event(&mut app, &ev),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Closed) => {
                    // Store is gone, quit
                    return Ok(());
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    // We missed some events; mark everything dirty
                    for ts in &mut app.tab_states {
                        ts.dirty = true;
                    }
                    break;
                }
            }
        }

        // If any tab is dirty, refresh data and redraw
        if app.any_dirty() {
            app.refresh_from_store(store).await;
            terminal.draw(|f| ui::draw(f, &mut app))?;
        }

        // Poll for crossterm events with a short timeout so we can
        // check store events frequently
        if let Some(ev) = poll_crossterm(Duration::from_millis(50))? {
            match ev {
                Event::Key(key) => {
                    match handle_key(&mut app, key) {
                        EventResult::Continue => {}
                        EventResult::Quit => return Ok(()),
                    }
                    // Redraw after input
                    terminal.draw(|f| ui::draw(f, &mut app))?;
                }
                Event::Resize(_, _) => {
                    terminal.draw(|f| ui::draw(f, &mut app))?;
                }
                _ => {}
            }
        }

        if app.should_quit {
            return Ok(());
        }
    }
}
