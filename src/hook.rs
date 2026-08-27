use std::io::{self, Write};

use yaca_transport::{
    AssistantContent, Event, Message, MessageUpdate, ReasoningContent,
    ToolCallDeltaContent, UserContent,
};

pub fn render_event(event: &Event) -> anyhow::Result<()> {
    match event {
        Event::SwitchConversation {
            messages: Ok(messages),
            ..
        } => {
            for message in messages {
                print_message(message);
            }
        }
        Event::SwitchConversation {
            messages: Err(err), ..
        } => eprintln!("{err}"),
        Event::NewMessage { message, .. } => print_message(message),
        Event::UpdateMessage { update, .. } => render_update(update)?,
        Event::TurnCompleted { error, .. } => {
            if !error.is_empty() {
                eprintln!("{error}");
            }
        }
        Event::AgentDestroyed { reason } => eprintln!("{reason}"),
    }
    io::stdout().flush()?;
    Ok(())
}

fn render_update(update: &MessageUpdate) -> anyhow::Result<()> {
    match update {
        MessageUpdate::Replace(message) => print_message(message),
        MessageUpdate::AssistantTextAppend(text) => print!("{}", text.text),
        MessageUpdate::AssistantReasoningAppend(text) => print!("{text}"),
        MessageUpdate::AssistantReasoningReplace(reasoning_contents) => {
            for content in reasoning_contents {
                print!("{}", reasoning_text(content));
            }
        }
        MessageUpdate::ToolCallReplace(tool_call) => {
            println!("Using {}", tool_call.function.name)
        }
        MessageUpdate::ToolCallAppend { id: _, content } => match content {
            ToolCallDeltaContent::Name(name) => println!("Using {name}: "),
            ToolCallDeltaContent::Delta(argument) => print!("{argument}"),
        },
    }
    Ok(())
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
        Message::Assistant { id: _, content } => {
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
                                .map(reasoning_text)
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

fn reasoning_text(content: &ReasoningContent) -> String {
    match content {
        ReasoningContent::Text { text, signature: _ } => text.clone(),
        ReasoningContent::Encrypted(text) => text.clone(),
        ReasoningContent::Redacted { data } => data.replace(|_| true, "*"),
        ReasoningContent::Summary(text) => text.clone(),
    }
}
