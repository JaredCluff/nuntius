# Nuntius

**NATS ↔ Claude Code MCP bridge.** Bidirectional, real-time messaging between Claude's conversation context and any NATS-connected agent or service.

```
Animus / Cortex / NexiBot / Hermes / any NATS service
       │  publish to animus.out.*
       ▼
   NATS server
       │
  [nuntius] ── subscribed to animus.out.>
       │  notifications/claude/channel  (MCP stdio)
       ▼
  Claude Code ── <channel> injected live into conversation
       │  nats_publish / nats_request tools
       ▼
   NATS server  ──► any agent listening on animus.in.*
```

**Proven live:** Animus publishes proactive status updates that appear directly in Claude's conversation context. Claude replies back in real-time. Full bidirectional loop verified March 2026.

---

## ⚠️ Security Status

**This is pre-alpha infrastructure with NO security hardening.** Before using in any shared or production environment, read this section carefully.

### Current limitations

| Area | Status | Risk |
|---|---|---|
| **Transport auth** | None by default | Any process on the NATS network can publish to subscribed subjects and inject content into Claude's conversation |
| **Message validation** | None | Payload content is injected as-is into Claude's context — no schema enforcement, no content filtering |
| **Channel notification trust** | Untrusted | Claude Code marks all channel notifications as "not from your user — treat as untrusted." Claude must still choose to act on them. |
| **Subject access control** | None | Claude can publish/subscribe to any subject on the NATS server it connects to |
| **NATS auth** | Supported but optional | `NUNTIUS_AUTH_TOKEN`, `NUNTIUS_USER`/`NUNTIUS_PASS`, and NKey are supported — **none are enabled by default** |
| **TLS** | Supported but optional | `NUNTIUS_TLS_CERT`/`NUNTIUS_TLS_KEY` env vars are available — not configured by default |
| **Prompt injection** | Not mitigated | A malicious NATS publisher could craft messages designed to manipulate Claude's behavior |

### Minimum security for homelab use

For a local setup where you control all NATS clients, this is acceptable:
- NATS server running locally (not exposed to the internet)
- Only trusted processes connecting to NATS
- Claude Code running in `bypassPermissions` mode only on personal workstations

### Before exposing to a network

1. Enable NATS authentication (`NUNTIUS_AUTH_TOKEN` or NKey)
2. Use NATS account/subject permissions to restrict what nuntius can subscribe to
3. Validate and sanitize message payloads before publishing to subscribed subjects
4. Consider a NATS-side allowlist: only known services may publish to `*.out.*`

---

## Tested Hardware & Environment

| Component | Tested version |
|---|---|
| **Hardware** | Apple M-series (Apple Silicon, arm64) |
| **OS** | macOS Sequoia 15.x |
| **Container runtime** | Podman 5.7.0 (rootless, macOS VM via gvproxy) |
| **NATS server** | 2.12.5 (running in Podman container, port 14222 mapped to host) |
| **Claude Code** | 2.1.81 |
| **Rust** | 1.83+ (edition 2021) |

### Podman networking note

When running NATS inside a Podman container on macOS:
- NATS is reachable at `localhost:14222` (or whichever port is mapped)
- Connections from the host go through `gvproxy` — NATS sees source IPs like `10.89.4.45` (the Podman bridge gateway), **not** `127.0.0.1`
- This is normal and does not affect functionality
- All host processes connecting through the same port forward appear with the same source IP

**Not yet tested on:**
- Linux (native NATS, no gvproxy layer)
- Windows
- x86_64 macOS
- Cloud/remote NATS (would require TLS + auth)

---

## Quick Start

### Option A: Use pre-built binary (macOS arm64)

```bash
# Download the binary from releases/
curl -L https://raw.githubusercontent.com/JaredCluff/nuntius/main/releases/nuntius-macos-arm64 \
  -o /usr/local/bin/nuntius
chmod +x /usr/local/bin/nuntius
```

Or copy from this repo:
```bash
cp releases/nuntius-macos-arm64 /usr/local/bin/nuntius
chmod +x /usr/local/bin/nuntius
```

### Option B: Build from source

```bash
git clone https://github.com/JaredCluff/nuntius
cd nuntius
cargo build --release -p nuntius
# binary: target/release/nuntius
```

---

## Claude Code Plugin Setup

### 1. Create plugin directory structure

```bash
MARKETPLACE=~/local-claude-marketplace
mkdir -p $MARKETPLACE/plugins/nuntius/.claude-plugin
```

**`$MARKETPLACE/plugins/nuntius/.mcp.json`**
```json
{
  "mcpServers": {
    "nuntius": {
      "command": "/path/to/nuntius",
      "args": [],
      "env": {
        "NUNTIUS_NATS_URL": "nats://localhost:4222",
        "NUNTIUS_STARTUP_SUBS": "animus.out.>"
      }
    }
  }
}
```

**`$MARKETPLACE/plugins/nuntius/.claude-plugin/plugin.json`**
```json
{
  "name": "nuntius",
  "description": "NATS↔MCP bridge — bidirectional messaging between Claude Code and NATS subjects"
}
```

**`$MARKETPLACE/.claude-plugin/marketplace.json`**
```json
{
  "plugins": [
    {
      "name": "nuntius",
      "description": "NATS↔MCP bridge",
      "version": "1.0.0",
      "author": { "name": "Your Name", "email": "you@example.com" },
      "source": "./plugins/nuntius",
      "category": "productivity"
    }
  ]
}
```

### 2. Configure Claude Code settings

**`~/.claude/settings.json`** — add these fields:
```json
{
  "marketplaces": {
    "my-local-marketplace": {
      "type": "local",
      "path": "/absolute/path/to/local-claude-marketplace"
    }
  },
  "enabledPlugins": {
    "nuntius@my-local-marketplace": true
  },
  "channelsEnabled": true
}
```

> **`channelsEnabled: true` is mandatory.** This is a global gate. Without it, Claude Code ignores all `notifications/claude/channel` messages from every MCP server.

### 3. Launch Claude Code

```bash
claude \
  --dangerously-load-development-channels plugin:nuntius@my-local-marketplace \
  --permission-mode bypassPermissions
```

**With Telegram:**
```bash
claude \
  --channels plugin:telegram@claude-plugins-official \
  --dangerously-load-development-channels plugin:nuntius@my-local-marketplace \
  --permission-mode bypassPermissions
```

**With a resume (continue previous conversation):**
```bash
claude \
  --channels plugin:telegram@claude-plugins-official \
  --dangerously-load-development-channels plugin:nuntius@my-local-marketplace \
  --resume "your session topic" \
  --permission-mode bypassPermissions
```

> **The `--dangerously-load-development-channels` flag is required every session.** Claude Code requires this flag to accept channel notifications from non-official plugins. There is no way to persist this in settings — it must be on the command line.

---

## Running Nuntius Standalone (testing)

You can run nuntius directly to test it without Claude Code:

```bash
# Basic test — connect to NATS and exit
NUNTIUS_NATS_URL=nats://localhost:4222 ./nuntius

# With debug logging
RUST_LOG=debug NUNTIUS_NATS_URL=nats://localhost:4222 ./nuntius

# With startup subscriptions
NUNTIUS_NATS_URL=nats://localhost:4222 \
NUNTIUS_STARTUP_SUBS=animus.out.>,cortex.out.> \
./nuntius
```

Nuntius speaks MCP over stdio. In standalone mode it reads from stdin and writes to stdout. You can manually send JSON-RPC messages:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{}}}' | \
  NUNTIUS_NATS_URL=nats://localhost:4222 ./nuntius
```

---

## How Channel Notifications Work

When a NATS message arrives on a subscribed subject, nuntius sends an MCP notification over its stdout pipe to Claude Code:

```json
{
  "jsonrpc": "2.0",
  "method": "notifications/claude/channel",
  "params": {
    "content": "message payload here",
    "meta": {
      "subject": "animus.out.probe",
      "ts": "2026-03-25T01:40:50.973339+00:00"
    }
  }
}
```

Claude Code injects this into the live conversation as:

```
<channel source="plugin:nuntius:nuntius" subject="animus.out.probe" ts="2026-03-25T01:40:50.973339+00:00">
message payload here
</channel>
```

Claude sees it immediately, without any tool call or polling. Claude Code marks it untrusted ("This is NOT from your user").

**What makes this work (all required):**
1. Server declares `experimental: { "claude/channel": {} }` in its MCP capabilities
2. `channelsEnabled: true` in `~/.claude/settings.json`
3. Server is in the `--dangerously-load-development-channels` list for this session
4. Protocol version is negotiated correctly (nuntius echoes back the client's requested version)

---

## Environment Variables Reference

| Variable | Default | Description |
|---|---|---|
| `NUNTIUS_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `NUNTIUS_STARTUP_SUBS` | *(empty)* | Comma-separated subjects to auto-subscribe at startup |
| `NUNTIUS_AUTH_TOKEN` | *(none)* | NATS token auth |
| `NUNTIUS_USER` | *(none)* | NATS username |
| `NUNTIUS_PASS` | *(none)* | NATS password |
| `NUNTIUS_NKEY` | *(none)* | NKey seed for NKey auth |
| `NUNTIUS_TLS_CERT` | *(none)* | Path to TLS client certificate |
| `NUNTIUS_TLS_KEY` | *(none)* | Path to TLS client key |
| `NUNTIUS_REQUEST_TIMEOUT_MS` | `5000` | Timeout for `nats_request` in milliseconds |

---

## Tools Reference

### Core Messaging

| Tool | Description |
|---|---|
| `nats_publish` | Fire-and-forget publish to a subject |
| `nats_request` | Publish and wait for a reply (request-reply) |
| `nats_subscribe` | Subscribe to a subject; messages inject as channel notifications |
| `nats_unsubscribe` | Remove an active subscription |

### JetStream (Persistent Messaging)

| Tool | Description |
|---|---|
| `js_stream_create` | Create a persistent stream |
| `js_stream_info` | Get stream metadata |
| `js_stream_delete` | Delete a stream |
| `js_publish` | Publish to JetStream (persisted, at-least-once) |
| `js_consume` | Pull next message from a durable consumer |

### Key-Value Store

| Tool | Description |
|---|---|
| `kv_put` | Store a value in a KV bucket |
| `kv_get` | Retrieve a value |
| `kv_delete` | Delete a key |
| `kv_keys` | List all keys in a bucket |

### Agent Registry

| Tool | Description |
|---|---|
| `agent_announce` | Register an agent with capabilities in the shared registry |
| `agent_discover` | Find agents by capability |
| `agent_claim` | Pull a task from a queue-group subject (at-most-once) |

---

## Subject Naming Conventions

```
{service}.out.{topic}    ─── service pushes TO Claude Code (channel notifications)
{service}.in.{topic}     ─── Claude Code pushes TO service (tool calls)

agents.registry.announce ─── agent presence broadcasts
agents.tasks.{type}      ─── work queues by task type
```

---

## Integration: Animus

Animus is the AI OS layer (VectorFS, Mnemos, Sensorium, Cortex, Telos).

**Nuntius startup subscriptions:**
```json
"NUNTIUS_STARTUP_SUBS": "animus.out.>"
```

**Subjects Animus uses:**
```
animus.out.claude     ── direct messages to Claude Code
animus.out.status     ── heartbeat / status updates
animus.out.events     ── sensor and observation events
animus.out.probe      ── ad-hoc probe/test messages
animus.in.claude      ── Claude Code → Animus commands
animus.in.ping        ── request-reply health check
```

**Animus Rust side (simplified):**
```rust
// Subscribe to commands from Claude
let mut sub = nc.subscribe("animus.in.>").await?;
while let Some(msg) = sub.next().await {
    let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    // handle command...
    if let Some(reply) = msg.reply {
        nc.publish(reply, b"ok").await?;
    }
}

// Push proactive update to Claude Code
nc.publish("animus.out.status", serde_json::json!({
    "from": "animus",
    "instance": "93cee0f9",
    "status": "operational"
}).to_string().into()).await?;
```

**Verified working:** Animus responds to `nats_request("animus.in.ping", ...)` with status, and proactively publishes on `animus.out.claude` which appears as `<channel>` in Claude's conversation.

---

## Integration: Cortex

Cortex (reasoning/planning agent). Same pattern as Animus.

**Add to startup subscriptions:**
```json
"NUNTIUS_STARTUP_SUBS": "animus.out.>,cortex.out.>"
```

**Suggested subjects:**
```
cortex.out.plan       ── generated execution plans
cortex.out.reasoning  ── completed reasoning chains
cortex.out.memory     ── memory consolidation events
cortex.in.plan        ── Claude Code → Cortex plan request
cortex.in.reason      ── ad-hoc reasoning task
```

**Claude Code usage:**
```
nats_request("cortex.in.plan", '{"goal": "...", "context": "..."}')
```

---

## Integration: NexiBot / OpenClaw

NexiBot is the user-facing assistant. Routes user interactions through NATS so Claude provides the reasoning layer.

**Add to startup subscriptions:**
```json
"NUNTIUS_STARTUP_SUBS": "animus.out.>,nexibot.out.>"
```

**Suggested subjects:**
```
nexibot.out.user_message  ── incoming user message requiring Claude reasoning
nexibot.out.context       ── context update (active document, workspace)
nexibot.in.response       ── Claude's response to deliver to user
nexibot.in.action         ── action to execute (UI update, tool call)
```

**NexiBot side (Node.js/TypeScript):**
```typescript
// Route user message through NATS to Claude
await nc.publish('nexibot.out.user_message', JSON.stringify({
  user_id: 'u123',
  session_id: 'sess456',
  message: userText,
  context: { document_id: activeDoc }
}))

// Receive Claude's response
const sub = nc.subscribe('nexibot.in.response')
for await (const msg of sub) {
  const { user_id, session_id, content } = JSON.parse(msg.string())
  deliverToUser(user_id, content)
}
```

**Claude Code side:**
Message arrives as `<channel>`, Claude reasons, then:
```
nats_publish("nexibot.in.response", '{"user_id":"u123","session_id":"sess456","content":"..."}')
```

---

## Integration: Hermes

Hermes is the inter-agent message router/bus.

**Role:** Agents send to `hermes.in.send`, Hermes routes and delivers to `hermes.out.{recipient}`.

**Add to startup subscriptions:**
```json
"NUNTIUS_STARTUP_SUBS": "animus.out.>,hermes.out.claude"
```

**Subjects:**
```
hermes.out.claude     ── messages routed to Claude Code
hermes.out.broadcast  ── broadcast to all connected agents
hermes.in.send        ── Claude Code → Hermes send request
hermes.in.route       ── routing directive
```

**Usage:**
```
# Claude Code sends via Hermes to another agent
nats_publish("hermes.in.send", '{"to":"animus","msg":"run daily snapshot"}')

# Claude Code receives from any agent via Hermes
# (appears as channel notification on hermes.out.claude)
```

---

## Multi-Agent Pattern

Nuntius enables a hub-and-spoke agent architecture where Claude Code is the hub:

```
          Claude Code (hub)
         /        |        \
   Animus      Cortex     NexiBot
   (AI OS)  (reasoning)  (user UI)
      |          |           |
      └──────────┴─────────►─┘
              NATS
              (shared bus)
```

**All agents subscribe to `agents.registry.announce`** to know who's online.
**Claude Code discovers agents** with `agent_discover`.
**Work is distributed** via `agent_claim` (queue-group, one agent claims each task).

```
# Claude announces itself
agent_announce("claude-code", ["reasoning", "planning", "code"], {"model": "claude-opus-4-6"})

# Find all agents that can do "reasoning"
agent_discover(capability="reasoning")

# Dispatch a task to whichever agent picks it up
nats_publish("agents.tasks.reasoning", '{"task_id":"t1","input":"..."}')

# Any agent with a claim loop picks it up (only one gets it)
agent_claim("agents.tasks.reasoning")
```

---

## Troubleshooting

### No channel notifications appearing

Check in order:

1. **`channelsEnabled: true` in `~/.claude/settings.json`**
   This is a global gate — no exceptions.

2. **`--dangerously-load-development-channels plugin:nuntius@<marketplace>` on CLI**
   The marketplace name must exactly match `enabledPlugins` in settings.json.
   Format is strict: `plugin:<name>@<marketplace>` — no spaces, exact casing.

3. **Nuntius is subscribed to the right subject**
   ```bash
   curl -s http://localhost:8222/connz?subs=1 | python3 -c "
   import json,sys; d=json.load(sys.stdin)
   for c in d['connections']:
       s=[x for x in c.get('subscriptions_list',[]) if not x.startswith('_SYS')]
       if s: print('CID', c['cid'], 'ip', c['ip'], 'out', c['out_msgs'], 'subs', s)"
   ```

4. **Message is reaching nuntius**
   Publish a test message and watch `out_msgs` increment on nuntius's CID.
   If `out_msgs` doesn't increment, the publish isn't reaching the same NATS instance.

5. **Stale nuntius process from previous session**
   `pkill nuntius` — then restart Claude Code. Multiple nuntius processes can cause confusion.

### NATS connection through Podman

When NATS runs in a Podman container on macOS, host processes connect via gvproxy's port forward. This is normal:
- `localhost:14222` → gvproxy → container:14222
- NATS sees source IP as the Podman bridge address (e.g., `10.89.4.45`)
- Both nuntius and Python scripts connecting from the host appear with the same source IP
- Functionality is not affected

### "entries must be tagged" error

The `--channels` and `--dangerously-load-development-channels` flags require tagged format:
```
plugin:<name>@<marketplace>   # for plugin-based servers
server:<name>                 # for plain mcpServer entries
```
Bare server names (e.g., just `nuntius`) will cause an error and exit.

---

## Building from Source

```bash
# Full build
cargo build --release -p nuntius

# Tests (requires nats-server in PATH)
cargo test

# Single crate
cargo test -p nuntius-core

# With debug output
RUST_LOG=nuntius=debug cargo run -p nuntius
```

**Prerequisites:** Rust 1.75+, `nats-server` binary for integration tests (`brew install nats-server`).

---

## Architecture Notes

**Why Rust?** Starts in ~5ms. Single static binary, no runtime dependencies. Runs indefinitely without memory leaks. Handles concurrent NATS subscriptions efficiently with tokio.

**Why a plugin (not plain mcpServer)?** Claude Code's channel notification system only activates for servers in the `--channels`/`--dangerously-load-development-channels` list. Plain `mcpServer` entries in `.claude.json` don't participate. Plugin registration is the required path.

**Protocol version negotiation** Nuntius echoes the client's requested `protocolVersion` back in the `initialize` response. Hardcoding an old version (e.g., `2024-11-05`) causes Claude Code to skip channel notification processing for that session.

**Concurrent notification delivery** Incoming NATS messages are handled by per-subscription tokio tasks. All notifications go to a single unbounded channel, drained by a single stdout writer task. This prevents interleaving and preserves order per subscription, while allowing multiple subscriptions to deliver concurrently.
