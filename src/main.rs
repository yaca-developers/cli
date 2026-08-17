use std::io::{self, BufRead};

use rig::prelude::*;
use rig::providers::anthropic;
use rig::{
    client::ProviderClient,
    memory::InMemoryConversationMemory,
    providers::{ollama, openai},
};
use yaca_core::{
    agent::{Agent, orchestrator::OrchestratorAgent},
    tools::Environment,
};

use crate::hook::TuiAgentLifecycleHook;

mod hook;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut agent = OrchestratorAgent::new(
        Environment::default(),
        anthropic::Client::from_env().unwrap(),
        "qwen3.8-max",
        InMemoryConversationMemory::new(),
        "main",
    )
    .with_lifecycle_hook(TuiAgentLifecycleHook)
    .await;
    let mut stdin = io::stdin().lock().lines();
    while let Some(Ok(line)) = stdin.next() {
        let result = agent.send_turn(Message::user(line), 32_000).await;
        if let Err(err) = result {
            eprintln!("{err}: {}", err.root_cause());
        }
    }
}
