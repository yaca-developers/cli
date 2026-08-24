# IPC plan: decoupling yaca-core from yaca-cli

Status: proposal
Scope: make yaca-core a standalone daemon exposing the agent engine over IPC,
with yaca-cli as a thin client. Two transports, both mandatory: Unix domain
socket (local default) and WebSocket (TCP, for remote/forwarded use).

## 1. Current coupling (as-is)

yaca-cli links yaca-core directly (`yaca-core = { path = "../yaca-core" }`) and
runs the full agent engine in-process:

- `yaca-cli/src/main.rs` constructs `OrchestratorAgent` via
  `OrchestratorParamsBuilder`, picking the provider (`openrouter`), model name,
  memory (`InMemoryConversationMemory`), and MCP transports (Kagi) itself.
- `yaca-cli/src/hook.rs` implements `yaca_core::agent::AgentLifecycleHook`
  (`TuiAgentLifecycleHook`) and registers it with
  `OrchestratorAgent::with_lifecycle_hook`.

The semantic surface that must be moved across a process boundary is small:

| Direction | In-process API (yaca-core) | Wire equivalent |
|---|---|---|
| client -> server | `Agent::send_turn(message, max_tokens)` | request `SendTurn` |
| client -> server | `Agent::load_conversation(id)` | request `LoadConversation` |
| client -> server | `Agent::conversation_id()` | request `GetConversationId` |
| client -> server | (new) create/destroy an agent instance | `CreateAgent`, `DestroyAgent` |
| server -> client | `AgentLifecycleHook::on_switch_conversation(id, Result<Vec<Message>, MemoryError>)` | event `SwitchConversation` |
| server -> client | `AgentLifecycleHook::on_new_message(index, Message)` | event `NewMessage` |
| server -> client | `AgentLifecycleHook::on_update_message(index, MessageUpdate)` | event `UpdateMessage` |

Serde feasibility (verified against rig-core 0.42.0 sources):
`rig::message::Message` (`completion/message.rs`), `rig::agent::Text`,
`ToolCall`, `ReasoningContent`, and `rig::streaming::ToolCallDeltaContent`
(`streaming/mod.rs`) all derive `Serialize`/`Deserialize`. The two gaps:

- `yaca_core::agent::MessageUpdate` needs `#[derive(Serialize, Deserialize)]`
  added (all its leaf types are serde-capable once rig is, per above).
- `rig::memory::MemoryError` is NOT serde-capable; on the wire,
  `on_switch_conversation`'s `Result<Vec<Message>, MemoryError>` becomes
  `Result<Vec<Message>, String>` (rendered error text).

## 2. Target architecture

    +------------------+        +------------------------------+        +-----------------+
    | yaca-cli         |        | yaca-daemon (bin in          |        | model providers |
    | UI/renderer only | <====> | yaca-core): owns             | <====> | MCP servers     |
    |                  |  IPC   | OrchestratorAgent, Shell,    |        | (daemon side)   |
    +------------------+        | MCP clients                  |        +-----------------+
            ^                   +------------------------------+
            | transports: unix socket AND websocket, same protocol on both

- **Server**: new binary `yaca-daemon` inside the yaca-core repo
  (`src/bin/daemon.rs`). It owns provider credentials, MCP server
  connections, the Shell tool, memory backends, and all `OrchestratorAgent`
  instances.
- **Client**: yaca-cli drops agent construction. It connects to a daemon
  endpoint, issues requests, and renders server-pushed events. The
  hook-rendering logic in `hook.rs` is kept, but re-driven from deserialized
  events instead of trait callbacks.
- **Shared protocol**: protocol types live in yaca-core under `ipc::protocol`
  (Phase 1: feature-gated `ipc` in yaca-core; both daemon and CLI use it).
  Phase 2 option: extract into a small `yaca-ipc` crate so the CLI does not
  need yaca-core at all. Decision deferred; the protocol module is written to
  be extractable (no deps outside serde/rig/tokio traits).

## 3. Wire protocol

### 3.1 Framing

One codec, two bindings. Encoding: JSON, one value per frame.

- Unix socket: newline-delimited JSON (NDJSON). One JSON value per line,
  UTF-8, `\n` terminator. Simple to implement with `tokio_util::codec::LinesCodec`
  or a manual read buffer; trivially debuggable with `socat`/`nc`.
- WebSocket: one Text message = one JSON value. No inner framing needed; the
  WS layer already frames. Ping/pong keepalive from tungstenite as-is.

Both transport adapters converge on a common internal abstraction:

```rust
// pseudo-signature
trait Duplex<T>: Stream<Item = Result<T, IpcError>> + Sink<T, Error = IpcError> {}
```

Server and client logic are written once against `Duplex<ClientMessage>` /
`Duplex<ServerMessage>`; `Transport::unix(stream)` and `Transport::ws(ws)` yield
adapters. No transport-specific code outside the adapter modules.

### 3.2 Envelope and message kinds

Version-negotiated; handshake is mandatory and first on every connection.

Client messages (`ClientMessage`):

```jsonc
// handshake (must be first)
{ "type": "hello", "protocol": 1, "client": "yaca-cli", "client_version": "0.1.0" }
// requests (id correlates responses)
{ "type": "request", "id": "u1", "method": "create_agent",
  "params": { "conversation_id": "main", "model": null } }
{ "type": "request", "id": "u2", "method": "send_turn",
  "params": { "agent_id": "a1", "message": { "role": "user", "content": [{ "type": "text", "text": "hi" }] }, "max_tokens": 32000 } }
{ "type": "request", "id": "u3", "method": "load_conversation",
  "params": { "agent_id": "a1", "conversation_id": "main" } }
{ "type": "request", "id": "u4", "method": "get_conversation_id",
  "params": { "agent_id": "a1" } }
{ "type": "request", "id": "u5", "method": "destroy_agent",
  "params": { "agent_id": "a1" } }
{ "type": "request", "id": "u6", "method": "ping" }
```

Server messages (`ServerMessage`):

```jsonc
{ "type": "hello", "protocol": 1, "server": "yaca-daemon", "server_version": "0.1.0" }
// response: exactly one per request, keyable by id
{ "type": "response", "id": "u2", "result": { "ok": true } }
{ "type": "response", "id": "u2", "error": { "message": "model error: ..." } }
// events: pushed, correlated to an agent, NOT to a request id.
{ "type": "event", "agent_id": "a1", "kind": "switch_conversation",
  "conversation_id": "main", "memory": { "ok": [ /* Vec<rig::Message> */ ] } }
{ "type": "event", "agent_id": "a1", "kind": "new_message",
  "index": 3, "message": { "role": "user", "content": [] } }
{ "type": "event", "agent_id": "a1", "kind": "update_message",
  "index": 4, "update": { "type": "assistant_text_append", "text": "Hello" } }
```

Wire shape of `MessageUpdate` (mirrors `yaca_core::agent::MessageUpdate`, tagged):

```rust
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageUpdate {           // becomes serde-capable in Phase 0
    Replace(Message),
    AssistantTextAppend(Text),
    AssistantReasoningAppend(String),
    AssistantReasoningReplace(Vec<ReasoningContent>),
    ToolCallReplace(ToolCall),
    ToolCallAppend { id: String, content: ToolCallDeltaContent },
}
```

Notes:

- `switch_conversation.memory` is `Result`-mapped: `{ "ok": [...] }` or
  `{ "err": "<display of MemoryError>" }`. Never serialize `MemoryError`.
- Events carry `agent_id` (not request id) because a turn spans many events;
  the final `response` to `send_turn` is the turn terminator for that request.
- Every error is `{ "message": String }` rendered with `anyhow`'s root-cause
  chain on the server (`err.root_cause()`), preserving today's UX
  (`Model error "{err}": ...`).

### 3.3 Why not JSON-RPC 2.0 verbatim

The shape is JSON-RPC-inspired (id, method, params) but deliberately minimal:
no batch mode, no positional params, an explicit push channel (`event`) with
its own correlation (`agent_id`). If we later adopt JSON-RPC 2.0 strictly
e.g. for `tower-jsonrpc`, nothing above changes semantically. Flagged as an
open question (section 9).

## 4. Server (daemon) design

New binary target in yaca-core: `src/bin/daemon.rs` (crate name `yaca-daemon`).
Adds dependencies (feature-gated `ipc`): `tokio-util` (framing), `futures`
(already present), `tokio-tungstenite` (ws), optionally `serde` (present).

### 4.1 Configuration

Provider/MCP wiring moves from the CLI to daemon config, priority:
CLI flag > env var > TOML file (`~/.config/yaca/daemon.toml`):

```toml
[provider]
type = "openrouter"        # enum: openrouter | anthropic | openai | ollama
api_key_env = "OPENROUTER_API_KEY"   # resolved daemon-side, keys never on IPC

[agent]
model = "opus-5"           # default when CreateAgent.model is null
max_tokens_default = 32000

[[mcp]]
uri = "https://mcp.kagi.com/mcp"
auth_env = "KAGI_API_KEY"  # env var holding the credential

[listen]
unix = "~/.yaca/daemon.sock"     # default; override with --listen-unix
ws = "127.0.0.1:8765"            # loopback only by default; --listen-ws off/on
```

This replaces the hardcoded `openrouter::Client::from_env()` + Kagi transport
in `yaca-cli/src/main.rs`.

### 4.2 Agent registry and concurrency

- `Registry: HashMap<AgentId, AgentHandle>` per daemon (agents are global, not
  per-connection, so a CLI reconnect can resume a conversation).
- `AgentHandle`: an actor. The `OrchestratorAgent` lives in its own tokio
  task with an mpsc inbox of commands (`SendTurn`, `LoadConversation`, ...).
  Sending a command returns a oneshot for the final response. This solves
  `&mut self` + interleaved event streaming without holding a `Mutex` across
  `send_turn`'s awaited stream.
- `send_turn` serialization is therefore inherent; a second `send_turn` for
  the same agent while one is in flight returns an immediate error
  (`agent busy`) — matches today's stdin-loop behavior (sequential turns).

### 4.3 Hook -> event bridge

Server implements `AgentLifecycleHook` once, as `IpcLifecycleHook`:

```rust
struct IpcLifecycleHook {
    agent_id: AgentId,
    events: mpsc::Sender<ServerMessage>, // per-connection event writer
}
```

Each `on_*` callback maps 1:1 to a `ServerMessage::event` (table in section 1)
and is forwarded into the per-connection event channel; the connection's
writer task drains it. `MemoryError` renders to `String` here, once.

On connection drop: `IpcLifecycleHook`'s sender fails -> events are dropped;
the agent itself survives (registry) unless `destroy_agent` is issued.

### 4.4 Listeners

- Unix: `tokio::net::UnixListener` on the configured path. On startup: remove
  stale socket, bind, `chmod 0600` (owner-only), and refuse to run if the path
  is a symlink to something unexpected. Cleanup on shutdown via `tokio::signal`.
- WebSocket: `tokio_tungstenite::accept_async` per TCP connection, default
  bind `127.0.0.1` only, disabled unless explicitly enabled in config/CLI.
- Both feed into the same `serve_connection(Duplex)` routine: read hello ->
  spawn request-dispatch loop + event-writer task.

## 5. Client (CLI) design

- `yaca-cli` keeps a path dependency on `yaca-core` but only uses
  `yaca_core::ipc::{protocol, client}` (feature `ipc`; no agent/tool features
  needed by the CLI). Phase 2 may extract `yaca-core/ipc` into its own crate
  (`yaca-ipc`) so the CLI's dependency graph drops rig's server-side stack.
- New `client` side API:

```rust
let conn = Endpoint::parse("unix:///Users/me/.yaca/daemon.sock") // or ws://127.0.0.1:8765
    .connect().await?;
let mut events = conn.events();                 // ServerMessage stream
let agent = conn.create_agent("main", None).await?;
agent.send_turn(Message::user(line), 32_000).await; // resolves on final response
```

- `hook.rs` is split into pure rendering functions:
  `render_event(ServerMessage::event)` reusing today's `print_message` and the
  `MessageUpdate` match arms verbatim. `TuiAgentLifecycleHook` (the trait impl)
  is deleted; rendering is driven by the client event stream.
- CLI arg surface: `--connect <uri>` (default `unix://$XDG_RUNTIME_DIR/yaca.sock`
  else `unix://~/.yaca/daemon.sock`), accepted schemes: `unix://`, `ws://`,
  `wss://`. Optional `--spawn` (Phase 4): if connect fails with NotFound on a
  unix path, spawn `yaca-daemon` and retry with backoff.

## 6. Security & robustness notes

- API keys and MCP credentials are resolved only on the daemon (config
  references env var names, never values). IPC carries no secrets.
- Unix socket: owner-only (`0600`), same-uid peers implicitly authorized.
- WebSocket: loopback default; if bound beyond loopback, require a bearer
  token in the `hello` (compare constant-time) and recommend `wss` behind a
  terminating proxy. Origin header unchecked (non-browser clients) — document.
- Handshake enforces protocol version; mismatched versions fail fast with a
  clear server `error` event.
- Backpressure: event channel bounded (e.g. 1024); if a client stops reading,
  server drops oldest `update_message` deltas before blocking a turn —
  rendering is loss-tolerant, history is not (full messages still arrive via
  `new_message`/`switch_conversation`).

## 7. Phased implementation

Phase 0 — core enablement (yaca-core, no IPC yet)
- derive serde on `agent::MessageUpdate`; round-trip tests vs. rig `Message`
  fixtures incl. all `MessageUpdate` variants.
- move provider/MCP/env wiring out of `yaca-cli/src/main.rs` into a
  `DaemonConfig` + `build_initializer(config) -> OrchestratorParams` helper in
  yaca-core (reused by the daemon bin; keeps CLI constructing nothing itself).

Phase 1 — protocol + in-process transport
- `ipc::protocol` (types above) + `ipc::transport::{unix, ws, memory}` +
  `ipc::server::{serve_connection, IpcLifecycleHook, Registry}`.
- `memory` transport: `tokio::io::duplex` backed; integration test drives a
  full `send_turn` against a mock `CompletionClient` (no network) over the same
  code path as sockets. Asserts event ordering: `new_message` -> 0..n
  `update_message` -> response.

Phase 2 — unix socket
- daemon listener + CLI `--connect unix://...`; end-to-end manual test:
  `yaca-daemon --listen-unix ...`, CLI converses, `kill` daemon mid-turn
  yields clean CLI error.

Phase 3 — websocket
- ws listener + `ws://` client endpoint; same end-to-end test matrix;
  verify both transports share one codec via `cargo test` reusing Phase 1
  assertions parameterized over transport.

Phase 4 — hardening & migration complete
- `--spawn`, reconnect/resume (registry survives disconnect), socket perms,
  version handshake, docs update (README, this doc -> guide), remove
  in-process path from CLI; yaca-cli no longer depends on
  `agent::orchestrator` at all.

## 8. Testing strategy

- Protocol golden tests: serialize each `ClientMessage`/`ServerMessage`
  variant to a pinned JSON string (catch wire regressions early).
- Transport matrix: for each transport (`memory`, `unix`, `ws`) run the same
  scripted conversation against a mock provider; assert identical event
  sequences.
- Concurrency: two `send_turn` to one agent -> second gets `agent busy`;
  `destroy_agent` during a turn cancels with a terminal `response`.
- Soak test (ws): 10k small `update_message` deltas, assert no reordering and
  bounded memory (event channel cap works).
- Manual matrix: daemon.sock + CLI; ws + CLI; ws from a second host with token.

## 9. Open questions / future work

- Extract `yaca-ipc` as its own crate (removes CLI's yaca-core dependency
  entirely) vs. feature-gating inside yaca-core. Recommend: feature-gate now,
  extract when a second client (non-Rust?) appears.
- Strict JSON-RPC 2.0 to unlock generic tooling (`--method` invocation from
  `jq`/`websocat` still works either way thanks to JSON framing).
- Multi-client broadcast: attach N CLIs to one agent (events to all).
- Binary encoding (postcard/messagepack) if `update_message` volume ever
  matters; the envelope's `protocol` field gives us an upgrade path.
- Conversation persistence backend choice stays opaque to IPC (daemon-side
  memory impl detail).

## 10. Acceptance criteria

1. `yaca-daemon` runs standalone; CLI in another terminal performs a full
   conversation over BOTH `unix://` and `ws://` endpoints with byte-identical
   event streams (ignoring ids).
2. `yaca-cli` builds without referencing `OrchestratorAgent`,
   `OrchestratorParams*`, providers, or MCP transports.
3. `cargo test` in yaca-core passes the transport matrix tests; hook semantics
   unchanged (ordering: switch -> new -> updates).
4. Killing the daemon mid-turn leaves the CLI with a clear error and a
   restartable state; killing the CLI leaves the agent alive (reconnect
   resumes via `load_conversation`).
