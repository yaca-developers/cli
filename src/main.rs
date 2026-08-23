use std::io::{self, BufRead};

use anyhow::Context;
use reqwest::header::{HeaderMap, HeaderName};
use rig::http_client::ReqwestClient;
use rig::prelude::*;
use rig::providers::anthropic;
use rig::{
    client::ProviderClient,
    memory::InMemoryConversationMemory,
    providers::{ollama, openai},
};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use yaca_core::agent::orchestrator::OrchestratorParamsBuilder;
use yaca_core::{
    agent::{Agent, orchestrator::OrchestratorAgent},
    tools::Environment,
};

use crate::hook::TuiAgentLifecycleHook;

mod hook;

async fn run_app() -> anyhow::Result<()> {
    let mut agent = OrchestratorAgent::new(
        OrchestratorParamsBuilder::default()
            .env(Environment::default())
            .model_name("qwen3.8-max")
            .client(anthropic::Client::from_env()?)
            .memory(InMemoryConversationMemory::new())
            .build()
            .unwrap()
            .with_mcp_servers(
                [StreamableHttpClientTransport::with_client(
                    ReqwestClient::default(),
                    StreamableHttpClientTransportConfig::with_uri("https://mcp.kagi.com/mcp")
                        .auth_header(
                            std::env::var("KAGI_API_KEY").with_context(|| "KAGI_API_KEY")?,
                        ),
                )],
                |_| true,
            )
            .await?,
        "main",
    )
    .with_lifecycle_hook(TuiAgentLifecycleHook)
    .await;
    let mut stdin = io::stdin().lock().lines();
    while let Some(Ok(line)) = stdin.next() {
        if line.trim().is_empty() {
            continue;
        }
        agent.send_turn(Message::user(line), 32_000).await?;
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    if let Err(err) = run_app().await {
        eprintln!("{err}: {}", err.root_cause());
    }
}
