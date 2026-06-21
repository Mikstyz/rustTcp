use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info};

const TIME_OUT_FROM_ACTIONS: Duration = Duration::from_millis(50);
const TIME_OUT_FROM_LOOP: Duration = Duration::from_millis(10);

// action
#[derive(Debug)]
pub enum ConsoleAction {
    TriggerActionW, // Ctrl + W
    Exit,           // (exit button) Esc
    None,           // other
}

pub async fn console_actions() -> Result<(), Box<dyn std::error::Error>> {
    // on raw mod terminal
    crossterm::terminal::enable_raw_mode()?;
    info!("Start terminal actions. Press Ctrl+W for action or Esc to exit.\r");

    let result = Box::pin(async {
        loop {
            if event::poll(TIME_OUT_FROM_ACTIONS)? {
                if let Event::Key(key_event) = event::read()? {
                    // send actoin to hot key func
                    match parse_hot_key(key_event) {
                        ConsoleAction::TriggerActionW => {
                            info!("[ACTION] Ctrl + W!\r");
                        }
                        ConsoleAction::Exit => {
                            info!("Exit command received\r");
                            break; // exit from loop
                        }
                        ConsoleAction::None => {} //ignore
                    }
                }
            }

            sleep(TIME_OUT_FROM_LOOP).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;

    // disable raw mod
    crossterm::terminal::disable_raw_mode()?;
    debug!("Raw mode disabled");

    result
}

fn parse_hot_key(event: KeyEvent) -> ConsoleAction {
    // Ctrl + W
    if event.code == KeyCode::Char('w') && event.modifiers.contains(KeyModifiers::CONTROL) {
        return ConsoleAction::TriggerActionW;
    }

    // save exit (esc)
    if event.code == KeyCode::Esc {
        return ConsoleAction::Exit;
    }

    ConsoleAction::None
}
