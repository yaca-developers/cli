use anyhow::Context;
use rig::{
    message::{AssistantContent, ReasoningContent, UserContent},
    prelude::*,
    streaming::ToolCallDeltaContent,
};
use std::io::{self, Write};
use tokio::io::AsyncWriteExt;
use yaca_core::agent::{AgentLifecycleHook, MessageUpdate};

pub struct TuiAgentLifecycleHook;

impl AgentLifecycleHook for TuiAgentLifecycleHook {
    async fn on_switch_conversation(
        &self,
        id: &str,
        memory: Result<Vec<rig::prelude::Message>, rig::memory::MemoryError>,
    ) -> anyhow::Result<()> {
        match memory {
            Ok(memories) => {
                for message in memories {
                    print_message(&message);
                }
            }
            Err(err) => eprintln!("{err}"),
        }
        Ok(())
    }

    async fn on_new_message(
        &self,
        index: usize,
        message: &rig::prelude::Message,
    ) -> anyhow::Result<()> {
        print_message(message);
        Ok(())
    }

    async fn on_update_message(
        &self,
        index: usize,
        message: &yaca_core::agent::MessageUpdate,
    ) -> anyhow::Result<()> {
        match message {
            MessageUpdate::Replace(message) => {
                // TODO
            }
            MessageUpdate::AssistantTextAppend(text) => {
                print!("{}", text.text)
            }
            MessageUpdate::AssistantReasoningAppend(text) => print!("{}", text),
            MessageUpdate::AssistantReasoningReplace(reasoning_contents) => {
                // TODO
            }
            MessageUpdate::ToolCallReplace(tool_call) => {
                println!(
                    "Used {}: {}",
                    tool_call.function.name, tool_call.function.arguments
                );
            }
            MessageUpdate::ToolCallAppend { id, content } => match content {
                ToolCallDeltaContent::Name(name) => println!("Using {name}: "),
                ToolCallDeltaContent::Delta(argument) => print!("{argument}"),
            },
        }
        io::stdout().flush().with_context(|| "flushing stdout")
    }
}

fn print_message(message: &Message) {
    match message {
        Message::System { content } => println!("System: {content}"),
        Message::User { content } => {
            print!("User: ");
            for item in content.iter() {
                match item {
                    UserContent::Text(text) => println!("{text}"),
                    _ => println!("[unsupported]"),
                }
            }
        }
        Message::Assistant { id, content } => {
            print!("Assistant: ");
            for item in content.iter() {
                match item {
                    AssistantContent::Text(text) => println!("{text}"),
                    AssistantContent::ToolCall(tool_call) => {
                        println!("Used {}", tool_call.function.name)
                    }
                    AssistantContent::Reasoning(reasoning) => {
                        println!(
                            "thinking ({})",
                            reasoning
                                .content
                                .iter()
                                .map(|it| match it {
                                    ReasoningContent::Text { text, signature: _ } => text.clone(),
                                    ReasoningContent::Encrypted(text) => text.clone(),
                                    ReasoningContent::Redacted { data } =>
                                        data.replace(|_| true, "*"),
                                    ReasoningContent::Summary(text) => text.clone(),
                                    _ => "".into(),
                                })
                                .collect::<Vec<_>>()
                                .join("\n")
                        );
                    }
                    AssistantContent::Image(_) => println!("[image]"),
                }
            }
        }
    }
}
