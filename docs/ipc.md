# IPC plan: decoupling yaca-core from yaca-cli (revision 3)

Status: proposal, revision 3 — incorporates second-round review feedback:

1. Codecs are generated with `tonic-build`; revision 2's hand-written
   prost-codec structs are dropped (only the rig<->wire *conversions* are
   hand-written). `bytes` = canonical-JSON payloads are retained.
2. Event lifecycle fixed: every `Subscribe` stream opens with a synthesized
   `SwitchConversation` snapshot. This closes the create-before-subscribe
   race (the initial `on_switch_conversation` fires inside
   `with_lifecycle_hook`, before any client can be subscribed) and makes
   reconnect/resume the same code path as first connect. Separately,
   `IpcLifecycleHook` is specified as infallible: in yaca-core a hook error
   aborts the turn, so transport failures must never surface as hook errors.
3. Wire types (including `MessageUpdate`) move into `yaca-transport`, which
   depends on rig + serde only; `yaca-core` re-exports them. The CLI depends
   on `yaca-transport` alone — no `yaca-core`, and no direct `rig`
   dependency (replacing today's `rig = "*"` wildcard, which with separate
   lockfiles would invite payload skew).
4. Payload versioning handshake added: the proto package versions the
   envelope, but JSON-in-`bytes` payloads are versioned by rig's serde, so
   client and daemon exchange a `payload_version` at `CreateAgent` and
   refuse skew.
5. WebSocket transport demoted to future work: no browser client exists and
   the goals (§0) are served by the unix socket. The `WsH2Adapter` sketch is
   retained in Appendix A.
6. Workspace consolidation is decided (single repo, layout (a)) and is a
   prerequisite of Phase 0, not an open question.

## 0. Goals and non-goals

Goals:

- G1. Lifetime/crash isolation: killing the CLI must not kill an in-flight
  turn; an agent-engine crash must not take the UI down with it.
- G2. Multiple frontends (the TUI now; others later) over one daemon that
  holds providers, MCP connections, and credentials.
- G3. A single protocol crate as the contract, so CLI and daemon can be
  versioned and released independently.
- G4. Secrets confined to the daemon process; nothing secret crosses IPC.

Non-goals (v1):

- Browser clients, multi-user authorization (same-uid local use only),
  remote hosts.
- Persistent conversation storage: the memory backend stays
  `InMemoryConversationMemory`, so a daemon restart loses conversations
  (§4.2, §9).
- Per-request provider switching: one daemon = one provider (§4.1).

These goals adjudicate the design: unix socket suffices (WS waits for a web
frontend), the registry attaches idempotently per conversation (G1/G2:
reconnect must find the previous agent), and `Subscribe` synthesizes a
snapshot (G1: a reconnected client renders current state with no special
casing).

## 1. Current coupling (as-is)

yaca-cli links yaca-core directly (`yaca-core = { path = "../yaca-core" }`) and
runs the full agent engine in-process:

- `yaca-cli/src/main.rs` constructs `OrchestratorAgent` via
  `OrchestratorParamsBuilder`, choosing provider (`openrouter`), model name,
  memory (`InMemoryConversationMemory`), and MCP transports (Kagi) itself.
- `yaca-cli/src/hook.rs` implements `yaca_core::agent::AgentLifecycleHook`
  (`TuiAgentLifecycleHook`) and registers it via
  `OrchestratorAgent::with_lifecycle_hook`.

The semantic surface that must move across a process boundary is small:

| Direction        | In-process API (yaca-core)                                | gRPC equivalent                                  |
|------------------|-----------------------------------------------------------|--------------------------------------------------|
| client -> server | `Agent::send_turn(message, max_tokens)`                   | unary `AgentService/SendTurn`                    |
| client -> server | `Agent::load_conversation(id)`                            | unary `AgentService/LoadConversation`            |
| client -> server | `Agent::conversation_id()`                                | unary `AgentService/GetConversation`             |
| client -> server | (new) create/destroy an agent instance                    | unary `AgentService/CreateAgent`, `DestroyAgent` |
| client -> server | (new) abort an in-flight turn (Ctrl-C)                    | unary `AgentService/CancelTurn`                  |
| server -> client | `AgentLifecycleHook::on_switch_conversation(id, Result<Vec<Message>, _>)` | item on server-stream `Subscribe` (snapshot-first, §4.3) |
| server -> client | `AgentLifecycleHook::on_new_message(index, Message)`      | item on server-stream `Subscribe`                |
| server -> client | `AgentLifecycleHook::on_update_message(index, MessageUpdate)` | item on server-stream `Subscribe`            |

Two behaviors of the current in-process core that the wire design must
respect (verified in `yaca-core/src/agent/orchestrator.rs`):

- **Hook errors abort the turn.** `send_turn` propagates `on_new_message` /
  `on_update_message` failures with `?`; a failing renderer kills the turn.
  Over IPC the hook must therefore never fail on transport errors (§4.3).
- **Lifecycle quirks.** `with_lifecycle_hook` fires `on_switch_conversation`
  during registration — before any network client could be subscribed. And
  `on_update_message` is invoked with the index of the *user* message that
  opened the turn (the assistant message lands at `index + 1` in memory;
  `conversation_len` is bumped before the stream starts, so a failed turn
  still advances it). These are pinned on the wire (§3.2) rather than
  silently "fixed" in the refactor.

Serde feasibility (verified against rig-core 0.42.0 sources):
`rig::message::Message` (`completion/message.rs`), `Text`, `ToolCall`,
`ReasoningContent`, `rig::streaming::ToolCallDeltaContent`
(`streaming/mod.rs`) all derive `Serialize`/`Deserialize`. The two gaps:

- `MessageUpdate` needs `#[derive(Serialize, Deserialize)]` — it gets them
  by moving into `yaca-transport` (§2); all leaf types are serde-capable per
  above.
- `rig::memory::MemoryError` is NOT serde-capable; on the wire,
  `on_switch_conversation`'s `Result<Vec<Message>, MemoryError>` becomes
  `Result<Vec<Message>, String>` (rendered text), matching how the CLI already
  prints it with `eprintln!("{err}")`.

gRPC consequence: protobuf cannot natively encode `rig`/`yaca-core` Rust
types, and rig's schema carries provider-specific maps (e.g.
`Text.additional_params`), so hand-mirroring the whole rig tree in `.proto`
would duplicate rig's serde surface and rot. Instead each rig-bearing field
is a single `bytes` field carrying canonical JSON (serde_json) of the Rust
value; only plain scalars/ids/statuses are native protobuf fields. The
structs themselves are generated by `tonic-build` (§3.2). Section 3.4 covers
the trade-off, the versioning consequence, and the alternative (fully
hand-mirrored oneof tree).

## 2. Target architecture (crate graph)

```
            +------------------+         +-----------------------+        +-------------------+
            | yaca-cli         |         | yaca-agent (bin)      |        | model providers   |
            | UI/renderer only | <=====> | OrchestratorAgent,    | <====> | MCP servers       |
            |                  |  gRPC   | Shell, MCP clients    |        | (daemon side)     |
            +------------------+         +-----------------------+        +-------------------+
                    |                      |           |
                    |                      v           |
                    |               +-------------+    |
                    |               | yaca-core   |    |
                    |               | (LIB)       |    |
                    |               +-------------+    |
                    |                      |           |
                    v                      v           v
            +--------------------------------------------------+
            | yaca-transport: proto, tonic stubs, wire types   |
            | (MessageUpdate), rig re-exports.                 |
            | Deps: rig, serde, prost, tonic only.             |
            +--------------------------------------------------+
```

Dependency direction (no cycles):

- `yaca-transport` (the IPC contract): the single source of truth for the
  protocol. Depends on rig (types), serde, prost, tonic — NOT on
  `yaca-core`. It owns the wire-convertible types, including
  `MessageUpdate` (with serde derives from day one), and re-exports the rig
  types the wire uses, so clients never depend on rig directly: the
  transport crate is the single source of the rig version (this replaces
  the CLI's `rig = "*"`). It defines:
  - the `.proto` service + `tonic-build`-generated structs/stubs,
  - hand-written conversions between wire structs and rig/yaca values,
  - the unix-socket bindings (client + server),
  - the `AgentService` client facade used by the CLI,
  - the `AgentService` server trait the daemon implements.
- `yaca-core` (library, unchanged in role): owns the agent engine and the
  hook semantics. Depends on `yaca-transport` for `MessageUpdate`, which it
  re-exports as `yaca_core::agent::MessageUpdate` — existing core and CLI
  source keeps compiling unchanged.
- `yaca-agent` (new binary): the daemon. Depends on `yaca-core` (to run
  agents) and `yaca-transport` (to expose them). Owns provider credentials,
  MCP connections, Shell, memory backends.
- `yaca-cli` (existing, now thin): depends on `yaca-transport` ONLY. No
  `yaca-core`, no `rig`, no agent construction, no providers, no MCP
  transport code. (The CLI renders the full rig message tree —
  `Message`/`UserContent`/`AssistantContent`/`ReasoningContent`/
  `ToolCallDeltaContent` — which it now gets via the transport re-export.)

Physical placement — decided: consolidate into one cargo workspace repo,
`yaca/{core,transport,agent,cli}`. Rationale: `yaca-transport` is needed
from Phase 0 (yaca-core depends on it), cross-crate CI and one lockfile
make payload-version skew at least build-time detectable, and refactors
stay atomic. Multi-repo is rejected for now; the crates can be split back
out later because the protocol boundary is a crate, not a repo.

## 3. Wire protocol (gRPC)

### 3.1 Service definition

`yaca-transport/proto/yaca/agent/v1/agent.proto`:

```proto
syntax = "proto3";
package yaca.agent.v1;

service AgentService {
  // Idempotent: returns the existing agent if conversation_id is already
  // active. This is the reconnect/resume path (4.2).
  rpc CreateAgent(CreateAgentRequest) returns (CreateAgentResponse);
  rpc DestroyAgent(DestroyAgentRequest) returns (DestroyAgentResponse);
  rpc SendTurn(SendTurnRequest) returns (SendTurnResponse);
  rpc CancelTurn(CancelTurnRequest) returns (CancelTurnResponse);
  rpc LoadConversation(LoadConversationRequest) returns (LoadConversationResponse);
  rpc GetConversation(GetConversationRequest) returns (GetConversationResponse);
  // Server-push channel carrying the AgentLifecycleHook callbacks.
  // The first item on every subscription is a synthesized
  // SwitchConversation snapshot of current state (4.3).
  rpc Subscribe(SubscribeRequest) returns (stream AgentEvent);
}
```

Why a separate `Subscribe` stream instead of folding events into
`SendTurn(...) returns (stream ...)`: events are per-agent, not per-request
(load/switch also emit, and a future multi-client broadcast wants one
subscription). The unary `SendTurn` response remains the per-turn terminator
**for the caller**; on the event stream the terminator is `TurnCompleted`
(§3.2), because HTTP/2 does not order the unary response against the last
events on a different stream — a renderer that re-prompts on the unary
response alone can race the final `UpdateMessage`s.

### 3.2 Messages (generated by tonic-build)

Structs, client stub, and server trait are all generated with `tonic-build`
(using `protoc-bin-vendored`, so no system `protoc` is required). The
hand-written code in `yaca-transport` is limited to *conversions* between
these wire types and rig/yaca values, plus the transport bindings (§3.3).
Every rig-bearing payload is a `bytes` field holding canonical JSON of the
corresponding Rust value; plain scalars/ids are native protobuf fields. This
keeps the schema the stable contract while letting us carry rig's
provider-extensible trees (`additional_params`) without remapping every
provider variant.

```proto
message CreateAgentRequest {
  string conversation_id = 1;
  string model = 2;            // empty -> daemon default; provider is fixed per daemon (4.1)
  string payload_version = 3;  // yaca-transport semver + rig version; see 3.4
}
message CreateAgentResponse {
  string agent_id = 1;
  string payload_version = 2;  // server's, for diagnostics
}
message DestroyAgentRequest { string agent_id = 1; }
message DestroyAgentResponse {}

message SendTurnRequest {
  string agent_id = 1;
  bytes  message_json = 2;     // rig::message::Message as canonical JSON
  uint64 max_tokens = 3;
  string turn_id = 4;          // client-supplied, echoed on turn-scoped events
}
message SendTurnResponse {}    // errors via gRPC status; ordering barrier is TurnCompleted

message CancelTurnRequest { string agent_id = 1; }
message CancelTurnResponse {}  // best-effort; if a turn was in flight, a
                               // TurnCompleted{error: "cancelled"} follows

// Mirrors core behavior: returns OK even when the memory load fails — the
// failure arrives as SwitchConversation.memory_error. Fails with
// FAILED_PRECONDITION if the target conversation is owned by another live
// agent (4.2). Routed through the actor inbox, ordered with turns.
message LoadConversationRequest  { string agent_id = 1; string conversation_id = 2; }
message LoadConversationResponse {}

message GetConversationRequest   { string agent_id = 1; }
message GetConversationResponse  { string conversation_id = 1; }
message SubscribeRequest { string agent_id = 1; }

message AgentEvent {
  string agent_id = 1;
  oneof kind {
    SwitchConversation switch_conversation = 2;
    NewMessage         new_message         = 3;
    UpdateMessage      update_message      = 4;
    TurnCompleted      turn_completed      = 5;  // last event of a turn; ordering barrier
    AgentDestroyed     agent_destroyed     = 6;  // terminal; stream closes after
  }
  string turn_id = 7;  // set on turn-scoped kinds (new_message, update_message, turn_completed)
}

message SwitchConversation {
  string conversation_id = 1;
  // Result<Vec<rig::Message>, MemoryError> -> exactly one is set; memory error -> string
  repeated bytes messages_json = 2;  // one rig Message as JSON per element; set on Ok
  string         memory_error  = 3;  // set on Err (rendered)
}

// Emitted once per turn, for the opening USER message; the assistant content
// of the turn arrives as UpdateMessage. `index` is the message's position in
// the conversation.
message NewMessage { uint64 index = 1; bytes message_json = 2; }

// PINNED SEMANTICS (yaca-core 0.1; do not "fix" on the wire without a v2):
// during a turn, updates carry the SAME index as the opening NewMessage
// (the user message's index); the assistant message is persisted at index+1.
message UpdateMessage { uint64 index = 1; bytes update_json = 2; }  // MessageUpdate as JSON

message TurnCompleted  { string error = 1; }  // empty on success; rendered anyhow error otherwise
message AgentDestroyed { string reason = 1; }
```

`AgentEvent.kind` is the only native `oneof` and it is fixed. The rig values
inside `bytes` are parsed with serde on both sides; the golden tests pin both
the protobuf bytes and the payload JSON (§8).

### 3.3 Transports

tonic serves HTTP/2 over any `AsyncRead + AsyncWrite`:

- **Unix domain socket (the v1 transport)**: serve with
  `Server::builder().add_service(...).serve_with_incoming_shutdown(
  UnixListenerStream, shutdown)`; `tokio::net::UnixStream` needs a one-line
  `tonic::transport::server::Connected` impl. Client side:
  `Endpoint::from_static("http://localhost") /* dummy URI */
  .connect_with_connector(tower::service_fn(|_| UnixStream::connect(path)))`.
  No TLS, no TCP. Socket hygiene in §4.4.
- **In-process loopback for tests**: same service over `tokio::io::duplex`,
  so the transport matrix (§8) runs without touching the filesystem.
- WebSocket / TCP+H2: deferred to Appendix A (no browser client exists; the
  unix socket covers §0's goals).

Naming note: the CLI-facing entry point is `yaca_transport::connect(uri)` —
not `tonic::transport::Endpoint`, which cannot parse `unix://` and whose name
the facade should not reuse.

### 3.4 Why gRPC, the `bytes=JSON` trade-off, and payload versioning

Chosen over the prior JSON envelope for: typed service contract + generated
client/server plumbing, streaming (`Subscribe`) as a first-class primitive,
and HTTP/2 flow control. The one real cost is the rig-type mapping:

- **Chosen (`bytes=JSON`)**: the protobuf schema stays small and stable; rig
  schema evolution (new provider fields) does not force `.proto` churn;
  correctness of rig (de)serialization stays in serde where rig already tests
  it. Costs: payloads are not schema-introspectable on the wire, two
  encodings are in play, and — the sharp edge — **payload schema versioning
  moves out of protobuf's reach**. rig is deliberately tolerant on decode
  (unknown keys are "tolerated, never captured", per its `AdditionalParams`
  doctrine), which for us means *silent field loss* across versions;
  structural changes fail deserialization loudly.
- **Rejected for now (full oneof mirror)**: mirror `Message`/`UserContent`/
  `AssistantContent`/`ReasoningContent`/`ToolCall`/`ToolCallDeltaContent` as
  a native oneof tree. More idiomatic protobuf, no double encoding, and
  protobuf would then version the payloads — a genuine advantage, not just
  aesthetics. Still rejected at this scale because it duplicates rig's serde
  surface and must be re-touched for every provider-specific addition rig
  adds. Revisit if cross-version operation or payload introspection becomes
  a real requirement.

Because the envelope's proto package cannot version the JSON payloads, the
**payload version handshake** compensates: `CreateAgentRequest.payload_version`
is `"{yaca-transport semver}/{rig version}"`; the daemon compares it against
its own and rejects mismatch with `FAILED_PRECONDITION` naming the expected
version. `CreateAgentResponse.payload_version` carries the daemon's value for
diagnostics. Exact-match is the right default while both sides ship from one
workspace/lockfile; relax deliberately later.

Note on naming: rig's tree has no struct/enum literally named `OneOf` (its
`oneOf` mentions are JSON-schema strings in the Gemini provider), so the
`oneof` in `AgentEvent` is ours, not a rig mapping artifact.

## 4. Server (`yaca-agent` binary)

New binary crate `yaca-agent`. Depends on `yaca-core` + `yaca-transport`
(+ tonic, tokio). It is the process that used to live inside the CLI.

### 4.1 Configuration

Provider/MCP wiring moves from `yaca-cli/src/main.rs` to the agent binary,
priority flag > env > TOML (`~/.config/yaca/agent.toml`):

```toml
[provider]
type = "openrouter"                 # openrouter | anthropic | openai | ollama
api_key_env = "OPENROUTER_API_KEY"  # resolved agent-side; keys never on IPC

[agent]
model = "opus-5"                    # default when CreateAgentRequest.model empty
max_tokens_default = 32000

[[mcp]]
uri = "https://mcp.kagi.com/mcp"
auth_env = "KAGI_API_KEY"           # env var holding the credential

[listen]
unix = "~/.config/yaca/agent.sock"  # see 4.4 for the full default rule
```

This replaces the hardcoded `openrouter::Client::from_env()` + Kagi transport
currently in the CLI. **Scoping decision: one daemon = one provider.**
`CreateAgentRequest.model` may only override the model *name* within the
daemon's configured provider; there is no per-request provider field.
Implementation consequence (Phase 0): rig-core 0.42 has no dyn-compatible
`CompletionClient`/`CompletionModel` wrapper, so runtime provider selection
is an enum over provider-specific `OrchestratorParams`
(`enum ProviderParams { OpenRouter(..), Anthropic(..), .. }` implementing
`Initializer`; `OrchestratorAgent<ProviderParams>`).

Today's dev keys come from `.cargo/config.toml` `[env]` — live secret values
sitting in a build file. After the cut they move to the daemon's environment
(or a real secret store); `agent.toml` references env var *names* only, and
the CLI repo's `.cargo/config.toml` stops carrying keys at all.

### 4.2 Agent registry and concurrency

- `Registry { agents: HashMap<AgentId, AgentHandle>, owners:
  HashMap<ConversationId, AgentId> }`, global to the process (not
  per-connection) so a CLI reconnect can resume a conversation.
- **`CreateAgent` is idempotent per conversation**: if `conversation_id` is
  already active, it returns the existing `agent_id` (attach). This is the
  reconnect/resume path — the CLI never needs to know whether it spawned the
  daemon — and it structurally prevents two agents from interleaving writes
  into one conversation in the shared memory map.
- `LoadConversation` re-keys the ownership map: it fails with
  `FAILED_PRECONDITION` if the target conversation is owned by another live
  agent, and releases the agent's previous conversation on success.
- `AgentHandle`: an actor; the `OrchestratorAgent` lives in its own tokio
  task with an mpsc inbox (`SendTurn`, `CancelTurn`, `LoadConversation`,
  `SubscribeAttach`) and oneshot responders for the unary replies. This
  sidesteps holding a lock across `send_turn`'s awaited stream, and puts all
  conversation-mutating operations on one ordered queue.
- `send_turn` serialization is inherent; a second concurrent `SendTurn` for
  the same agent returns `Status::failed_precondition("agent busy")`, matching
  today's sequential stdin loop.
- **Cancellation**: each in-flight turn holds a `CancellationToken`.
  `CancelTurn` trips it (the actor drops the stream future; rig's stream is
  drop-cancellable). gRPC deadline-exceeded or client-side cancellation of
  the `SendTurn` unary maps to the same token. A client *process dying* does
  NOT cancel the turn (G1). A cancelled turn ends with
  `TurnCompleted{error: "cancelled"}` on the event stream and `CANCELLED`
  on the unary.
- **`DestroyAgent` is handled by the registry, not the inbox**: it aborts the
  actor task (`JoinHandle::abort`), which resolves the pending `SendTurn`
  responder as `CANCELLED`, emits terminal `AgentDestroyed` to subscribers
  and closes their streams, and removes both map entries. Mid-turn abort may
  leave the last assistant message partially appended in memory — accepted
  for v1, documented.
- The memory backend is `InMemoryConversationMemory` (per-process): a daemon
  restart loses all conversations. Reconnect/resume holds only while the
  daemon lives. A persistent backend is future work (§9).

### 4.3 Hook -> gRPC event bridge, and the `Subscribe` snapshot

One `AgentLifecycleHook` impl in `yaca-agent`:
`IpcLifecycleHook { agent_id, fanout: Fanout }`. Each callback maps 1:1 to an
`AgentEvent` (§3.2) pushed into the agent's fan-out. `MemoryError` is
rendered to `String` once, here.

**The hook is infallible by contract.** In yaca-core, a hook error aborts the
in-flight turn (`send_turn` propagates it). So `IpcLifecycleHook` logs and
swallows conversion/queue failures and always returns `Ok(())`. "Events drop,
agent survives" is therefore a property of the hook, not a happy accident —
and because the orchestrator *awaits the hook inline per stream item*, the
hook must also never block: all sends are `try_send`.

**Fan-out and drop policy.** Per-agent registry of subscriber senders, each a
bounded mpsc (e.g. 1024). `UpdateMessage` events may be dropped when a
subscriber's queue is full (rendering is loss-tolerant); if a *non-droppable*
event (`SwitchConversation`, `NewMessage`, `TurnCompleted`, `AgentDestroyed`)
cannot be queued, that subscription is terminated with
`RESOURCE_EXHAUSTED` — the client resubscribes and gets a fresh snapshot, so
history is never silently lost. (A plain `tokio::sync::broadcast` cannot
express this per-type policy — it drops oldest regardless of kind — hence an
explicit fan-out.)

**Subscribe snapshot.** `Subscribe` requests are routed through the actor
inbox (`SubscribeAttach`): the actor registers the new subscriber's sender
and immediately emits a *synthesized* `SwitchConversation` — current
`conversation_id` plus a fresh memory load — to that sender, after which live
events flow. Because registration, snapshot, and live emission are all
ordered by the single actor, there is no gap or duplication window. This also
disposes of the initial `on_switch_conversation` fired inside
`with_lifecycle_hook` during agent construction: it has no subscribers yet
and is dropped, by design, since the first `Subscribe` re-synthesizes it.

`TurnCompleted` is emitted when `send_turn` returns (success or error), with
the client-supplied `turn_id` echoed.

### 4.4 Listeners

- Unix: default path `$XDG_RUNTIME_DIR/yaca/agent.sock` when that variable is
  set, else `~/.config/yaca/agent.sock` (config and socket under one scheme;
  `$XDG_RUNTIME_DIR` is effectively never set on macOS, where the fallback
  always fires). Create the parent directory `0700` if missing; remove stale
  socket first; `chmod 0600`; refuse symlinked paths; clean up on shutdown
  via `tokio::signal`.
- HTTP/2 keepalive (PING) is enabled on the server so half-dead connections
  are reaped; cheap on unix sockets and required later off-loopback.
- WS/TCP listeners: deferred (Appendix A).

## 5. Client (`yaca-cli`)

- Depends on `yaca-transport` ONLY (client stub + wire types + rig
  re-exports). Its `Cargo.toml` drops `yaca-core`, `rig = "*"`, and `rmcp`.
- `yaca-transport::client` exposes:

```rust
let conn = yaca_transport::connect("unix:///Users/me/.config/yaca/agent.sock").await?;
let agent = conn.create_agent("main", None).await?;        // idempotent attach (4.2)
let mut events = conn.subscribe(agent.id()).await?;        // first item: SwitchConversation snapshot
agent.send_turn(Message::user(line), 32_000, turn_id).await?; // unary; errors via gRPC status
```

- `hook.rs` becomes pure rendering: `render_event(&AgentEvent)` reuses
  today's `print_message` and the `MessageUpdate` match arms verbatim
  (importing the rig tree via the transport re-export); the
  `TuiAgentLifecycleHook` trait impl is deleted.
- **Turn end = both signals**: the unary `SendTurn` response is the error
  channel; `TurnCompleted` on the event stream is the ordering barrier. The
  prompt loop waits for the unary, and the renderer drains events up to the
  matching `TurnCompleted{turn_id}` before re-prompting. (`SendTurn` with no
  active subscriber succeeds but renders nothing — documented; the normal
  flow subscribes first.)
- CLI surface: `--connect <uri>` (default `unix://$XDG_RUNTIME_DIR/yaca/
  agent.sock` when set, else `unix://~/.config/yaca/agent.sock`); `unix://`
  only for v1, `ws://`/`wss://` reserved. Optional `--spawn`: on unix-socket
  NotFound, spawn `yaca-agent` and retry with backoff.
- **Reconnect after CLI restart** is just the normal flow: idempotent
  `CreateAgent` re-attaches, `Subscribe`'s snapshot repaints the screen. No
  explicit `LoadConversation` needed.

## 6. Security & robustness (unix-only v1)

- API keys / MCP credentials resolved only in `yaca-agent` (config references
  env var names, never values). Nothing secret crosses IPC.
- Unix socket: `0600`, parent dir `0700`, same-uid peers implicitly
  authorized.
- Versioning: the *envelope* is versioned by the proto package
  (`yaca.agent.v1`; breaking change means `v2`; non-breaking evolution uses
  reserved field numbers). The *payloads* are versioned by the
  `payload_version` handshake (§3.4).
- Backpressure: H2 flow control plus the §4.3 bounded-queue policy —
  `update_message` droppable, everything else delivered or the subscription
  terminated loudly. The agent turn itself never stalls on a slow client.
- Non-loopback transports, bearer-token metadata auth, TLS-terminating
  proxies: deferred with the WS/TCP transports (Appendix A).

## 7. Phased implementation

Phase 0 — workspace consolidation + core enablement (no IPC)
- Merge into one cargo workspace repo `yaca/{core,transport,agent,cli}`
  (decided, §2); single lockfile.
- Create `yaca-transport` as a **types-only** crate (no tonic yet):
  `MessageUpdate` with serde derives + the wire-type definitions + rig
  re-exports. `yaca-core` depends on it and re-exports
  `yaca_core::agent::MessageUpdate` (existing imports keep compiling).
  Serde round-trip tests vs. rig `Message` fixtures for every
  `MessageUpdate` variant.
- Move provider/MCP/env wiring out of `yaca-cli/src/main.rs` into
  `AgentConfig` + `build_initializer(config) -> ProviderParams` in
  yaca-core (reused by `yaca-agent`). Provider selection is enum-dispatched
  (rig-core 0.42 has no dyn-compatible client/model wrapper, §4.1).

Phase 1 — transport crate (networking)
- `agent.proto` + `tonic-build` codegen (`protoc-bin-vendored`); hand-written
  conversions; unix binding (client `connect_with_connector`, server
  `serve_with_incoming_shutdown`); in-process `duplex` loopback for tests;
  `payload_version` handshake.
- Golden tests: every request/response/event variant encodes to a pinned
  protobuf byte string AND its rig-bearing fields encode to pinned payload
  JSON.

Phase 2 — `yaca-agent` over unix socket
- Registry + ownership map + actor + fan-out + `IpcLifecycleHook` +
  `Subscribe` snapshot; `CancelTurn`/`DestroyAgent` semantics per §4.2;
  serve tonic over unix; drive a full `send_turn` against a mock
  `CompletionClient` (no network) and assert event ordering
  `switch(snapshot) -> new -> updates -> turn_completed`.

Phase 3 — `yaca-cli` migration
- CLI switches to the client facade; `hook.rs` becomes `render_event`;
  the `TuiAgentLifecycleHook` impl is deleted; `--spawn`; reconnect/resume.
  CLI `Cargo.toml` drops `yaca-core`, `rig`, `rmcp`.

Phase 4 — hardening & cut-over
- Socket perms/dir modes, keepalive, `SendTurn` deadlines, docs; remove the
  in-process path so the CLI no longer references `OrchestratorAgent`/
  providers/MCP.

Future (not scheduled): WS/TCP transports + non-loopback auth (Appendix A);
persistent memory backend; multi-provider daemons / per-request provider
override; multi-client UX beyond the built-in fan-out.

## 8. Testing strategy

- Golden wire tests: pinned protobuf bytes + pinned payload JSON for every
  `AgentEvent`/request/response variant (catches schema AND payload
  regressions).
- Version handshake: mismatched `payload_version` -> `FAILED_PRECONDITION`.
- Transport matrix: the same scripted conversation against a mock provider
  over loopback-duplex and unix; assert identical `AgentEvent` sequences and
  the snapshot-first invariant on every `Subscribe`.
- Lifecycle: `Subscribe` before vs. after `CreateAgent` both yield
  snapshot-first; slow consumer: `update_message`s dropped, first
  non-droppable overflow terminates the subscription with
  `RESOURCE_EXHAUSTED`, and resubscribe restores full state via snapshot.
- Concurrency: two `SendTurn` on one agent -> second gets `agent busy`;
  `CancelTurn` mid-turn -> `TurnCompleted{error: "cancelled"}` + unary
  `CANCELLED`; `DestroyAgent` mid-turn -> pending unary `CANCELLED`,
  subscribers receive `AgentDestroyed` then stream close.
- Kill the CLI mid-turn: the turn completes; reattach via idempotent
  `CreateAgent` + `Subscribe` snapshot shows the final state.
- Hook infallibility: subscriber disconnect mid-turn does not fail the turn
  (regression test against core's hook-error propagation).
- Manual matrix: unix + CLI; daemon killed mid-turn -> clear CLI error and
  restartable state.

## 9. Open questions / future work

- Keep `bytes=JSON` payloads vs. investing in the full native oneof mirror
  of the rig tree (revisit if cross-version operation or payload
  introspection appears; note the mirror would restore protobuf versioning
  of payloads, §3.4).
- Persistent memory backend: daemon restart currently loses all
  conversations (`InMemoryConversationMemory`).
- WS/TCP transports and their auth story (Appendix A) — schedule when a web
  or remote frontend actually appears.
- Multi-client broadcast beyond the built-in fan-out (attach/detach UX,
  per-client render state).
- Multi-provider daemon, or per-request provider on `CreateAgent`.

## 10. Acceptance criteria

1. `yaca-agent` runs standalone; the CLI performs a full conversation over
   `unix://` with event order
   `switch(snapshot) -> new -> updates -> turn_completed`; the same sequence
   holds over the duplex test transport.
2. `yaca-cli` links `yaca-transport` ONLY: no `yaca-core`, no `rig`, no
   `OrchestratorAgent`/`OrchestratorParams*`, no providers, no MCP
   transports.
3. `cargo test` passes the transport matrix and lifecycle tests; hook
   ordering semantics unchanged, including the pinned update-index quirk
   (§3.2).
4. Killing `yaca-agent` mid-turn leaves the CLI with a clear error and a
   restartable state (conversation history is lost until a persistent
   backend lands — documented); killing the CLI leaves the agent alive, and
   restarting the CLI re-attaches via idempotent `CreateAgent` + `Subscribe`
   snapshot with no explicit `LoadConversation`.
5. Crate graph holds: `yaca-cli -> yaca-transport`,
   `yaca-agent -> {yaca-core, yaca-transport}`,
   `yaca-core -> yaca-transport`,
   `yaca-transport -> {rig, serde, prost, tonic}`; no cycles, and
   `yaca-core` is a plain library dependency of the daemon only.
6. `payload_version` mismatch is rejected at `CreateAgent` with
   `FAILED_PRECONDITION`.

## Appendix A — deferred: WebSocket / TCP transports

Motivation would be a browser/web frontend or a remote daemon host; neither
exists yet (§0), so these are out of v1 but the design is recorded.

- **WebSocket**: tonic expects an H2 byte stream; a WS connection is
  message-oriented, so `yaca-transport` would add a thin `WsH2Adapter`
  adapting tungstenite binary frames to an `AsyncRead + AsyncWrite` byte
  pipe (buffered read, write-flush per frame), then run the same tonic
  server/client over it. Bind loopback by default, disabled unless enabled.
  Beyond loopback: bearer token passed as gRPC metadata (`authorization`),
  TLS-terminating proxy for `wss`, Origin unchecked (non-browser clients) —
  documented. Add the soak test here: 10k small `update_message` events,
  assert ordering + bounded memory.
- **TCP + H2**: needs no adapter at all (tonic supports it natively) and is
  the cheaper option if a remote daemon is ever wanted without a browser
  client; same token-auth story.
- Either way the acceptance bar is the §8 transport matrix parameterized
  over the new binding, asserting identical `AgentEvent` sequences.
