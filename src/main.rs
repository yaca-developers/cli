use std::io::{self, BufRead};

use anyhow::Context;
use yaca_transport::{Event, Message};

mod hook;

fn default_uri() -> String {
    std::env::var("YACA_CONNECT").unwrap_or_else(|_| yaca_transport::default_unix_uri())
}

fn next_turn_id(counter: &mut u64) -> String {
    *counter += 1;
    format!("turn-{counter}")
}

async fn run_app() -> anyhow::Result<()> {
    let uri = default_uri();
    eprintln!("connecting to {uri}");
    let conn = yaca_transport::connect(&uri)
        .await
        .with_context(|| format!("connecting to {uri}"))?;
    let agent = conn.create_agent("main", None::<String>).await?;
    let mut events = conn.subscribe(agent.id()).await?;
    let mut turn_id_counter = 0u64;
    let stdin = io::stdin();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let current_turn_id = next_turn_id(&mut turn_id_counter);
        let send_result = agent
            .send_turn(Message::user(line), 32_000, current_turn_id.clone())
            .await;

        if let Err(err) = send_result {
            eprintln!("Model error: {err:#}");
        }

        while let Some(event) = events.next_event().await {
            let event = event?;
            let turn_completed = matches!(
                &event,
                Event::TurnCompleted { turn_id, .. } if turn_id == &current_turn_id
            );
            hook::render_event(&event)?;
            if turn_completed {
                break;
            }
        }
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    if let Err(err) = run_app().await {
        eprintln!("{err:#}");
    }
}
