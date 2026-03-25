# Nuntius

**Nuntius** is a [Model Context Protocol (MCP)](https://spec.modelcontextprotocol.io/) server that connects [Claude Code](https://claude.ai/claude-code) to [NATS](https://nats.io/), a high-performance messaging system. It gives Claude Code the ability to publish and subscribe to messages, work with persistent streams and key-value stores, participate in multi-agent coordination, and request permission gates before executing sensitive operations.

> **Name**: *Nuntius* is Latin for "messenger" or "envoy."

---

## Table of Contents

- [What It Does](#what-it-does)
- [Architecture](#architecture)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Build from Source](#build-from-source)
  - [Claude Code Integration](#claude-code-integration)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Authentication](#authentication)
  - [Multi-Instance Identity](#multi-instance-identity)
- [Tools Reference](#tools-reference)
  - [Core Messaging](#core-messaging)
    - [nats_publish](#nats_publish)
    - [nats_request](#nats_request)
    - [nats_subscribe](#nats_subscribe)
    - [nats_unsubscribe](#nats_unsubscribe)
  - [JetStream (Persistent Streams)](#jetstream-persistent-streams)
    - [js_publish](#js_publish)
    - [js_stream_create](#js_stream_create)
    - [js_stream_info](#js_stream_info)
    - [js_stream_delete](#js_stream_delete)
    - [js_consume](#js_consume)
  - [KV Store](#kv-store)
    - [kv_put](#kv_put)
    - [kv_get](#kv_get)
    - [kv_delete](#kv_delete)
    - [kv_keys](#kv_keys)
  - [Agent Coordination](#agent-coordination)
    - [agent_announce](#agent_announce)
    - [agent_discover](#agent_discover)
    - [agent_claim](#agent_claim)
    - [request_permission](#request_permission)
- [Inbound Messages (NATS → Claude)](#inbound-messages-nats--claude)
- [Subject Naming Conventions](#subject-naming-conventions)
- [Multi-Instance Architecture](#multi-instance-architecture)
  - [Agent Registry](#agent-registry)
  - [Heartbeat and TTL](#heartbeat-and-ttl)
  - [Targeting a Specific Instance](#targeting-a-specific-instance)
  - [Load-Balanced Worker Pools](#load-balanced-worker-pools)
- [Permission Gates](#permission-gates)
- [Startup Behavior](#startup-behavior)
- [Logging and Observability](#logging-and-observability)
- [Testing](#testing)
- [License](#license)

---

## What It Does

Nuntius exposes **17 tools** to Claude Code across four categories:

| Category | Tools | Purpose |
|---|---|---|
| Core Messaging | `nats_publish`, `nats_request`, `nats_subscribe`, `nats_unsubscribe` | Fire-and-forget, request-reply, event subscription |
| JetStream | `js_publish`, `js_stream_create`, `js_stream_info`, `js_stream_delete`, `js_consume` | Persistent, durable message streams |
| KV Store | `kv_put`, `kv_get`, `kv_delete`, `kv_keys` | Shared key-value state across agents |
| Agent Coordination | `agent_announce`, `agent_discover`, `agent_claim`, `request_permission` | Multi-agent registry, work distribution, permission gates |

Messages that arrive on any active subscription are automatically delivered into Claude's context as `<channel>` notifications, allowing Claude to react to external events in real time.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Claude Code (MCP client)                                           │
│                                                                     │
│   tool call: nats_publish(subject, payload) ──────────────────┐    │
│   tool call: nats_subscribe(subject)        ──────────────────┼──┐ │
│   <channel> notification ←─────────────────────────────────────┘  │ │
└────────────────────────────────────────────────────────────────┬───┘
                          stdin/stdout JSON-RPC 2.0              │
┌───────────────────────────────────────────────────────────────┐│
│  Nuntius (MCP server)                                         ││
│                                                               ││
│  ┌─────────────────────┐     ┌────────────────────────────┐  ││
│  │  MCP Protocol Layer │     │  NatsBridge                │  ││
│  │  - initialize        │────▶│  - publish / request       │  ││
│  │  - tools/list        │     │  - subscribe (w/ tasks)    │  ││
│  │  - tools/call        │     │  - notification channel    │  ││
│  └─────────────────────┘     └─────────────┬──────────────┘  ││
│                                             │                  ││
│  ┌─────────────────────┐                   │ notification     ││
│  │  Tool Registry (17) │                   │ sender           ││
│  │  core / js / kv     │                   ▼                  ││
│  │  agent coordination │     ┌────────────────────────────┐  ││
│  └─────────────────────┘     │  stdout writer task        │──┘│
│                               │  (serialized, no interleave)│   │
│                               └────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
                          TCP
┌──────────────────────────────────────────────────────────────────┐
│  NATS Server (with JetStream)                                    │
│                                                                  │
│  Subjects, Streams, KV Buckets, Request-Reply Inboxes           │
└──────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

- **Stdout is the MCP channel.** All logging goes to stderr. Nuntius never mixes protocol messages and logs.
- **Single stdout writer task.** A Tokio channel serializes all outgoing lines to prevent interleaving when NATS messages and tool responses arrive concurrently.
- **Subscriptions spawn background tasks.** Each subscription drives an independent Tokio task that forwards incoming NATS messages as MCP notifications. Killing one subscription does not affect others.
- **Stateless tool execution.** Each tool call receives the full `NatsBridge` context and operates independently. No shared mutable state between calls.

---

## Installation

### Prerequisites

- [Rust](https://rustup.rs/) 1.75 or later
- [NATS Server](https://docs.nats.io/running-a-nats-service/introduction/installation) 2.10 or later with JetStream enabled
- Claude Code with MCP server support

Install NATS server (macOS):

```bash
brew install nats-server
```

Start it with JetStream enabled:

```bash
nats-server -js
```

### Build from Source

```bash
git clone https://github.com/JaredCluff/nuntius.git
cd nuntius
cargo build --release
```

The binary will be at `target/release/nuntius`.

### Claude Code Integration

Add nuntius to your Claude Code MCP configuration. The recommended approach is to create a plugin config at a path Claude Code reads:

```json
{
  "mcpServers": {
    "nuntius": {
      "command": "/path/to/nuntius/target/release/nuntius",
      "args": [],
      "env": {
        "NUNTIUS_NATS_URL": "nats://localhost:4222",
        "NUNTIUS_STARTUP_SUBS": "myapp.events.>",
        "NUNTIUS_INSTANCE_ID": "main"
      }
    }
  }
}
```

Or add it directly to your Claude Code settings under `mcpServers`.

---

## Configuration

All configuration is via environment variables. There are no config files.

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NUNTIUS_NATS_URL` | `nats://localhost:4222` | NATS server address. Supports `nats://`, `tls://`, and `nats://user:pass@host:port` formats. |
| `NUNTIUS_AUTH_TOKEN` | *(none)* | Token-based authentication. Used when the NATS server requires a static token. |
| `NUNTIUS_USER` | *(none)* | Username for user/password authentication. |
| `NUNTIUS_PASS` | *(none)* | Password for user/password authentication. Must be used with `NUNTIUS_USER`. |
| `NUNTIUS_NKEY` | *(none)* | NKey seed for NKey-based authentication. |
| `NUNTIUS_TLS_CERT` | *(none)* | Path to a PEM-encoded TLS client certificate (for mutual TLS). |
| `NUNTIUS_TLS_KEY` | *(none)* | Path to the PEM-encoded private key for the TLS client certificate. |
| `NUNTIUS_STARTUP_SUBS` | *(none)* | Comma-separated list of NATS subjects to auto-subscribe at startup. Messages on these subjects will appear as `<channel>` notifications immediately. Example: `animus.out.>,alerts.>` |
| `NUNTIUS_REQUEST_TIMEOUT_MS` | `5000` | Default timeout in milliseconds for `nats_request` calls. |
| `NUNTIUS_INSTANCE_ID` | *(random 8-char UUID prefix)* | Stable identifier for this Claude Code instance. See [Multi-Instance Architecture](#multi-instance-architecture). |
| `RUST_LOG` | *(none)* | Controls log verbosity (to stderr). Examples: `info`, `nuntius=debug`, `nuntius_core=trace`. |

### Authentication

Authentication methods are evaluated in priority order:

1. **Token auth** — Set `NUNTIUS_AUTH_TOKEN`
2. **User/password auth** — Set both `NUNTIUS_USER` and `NUNTIUS_PASS`
3. **NKey auth** — Set `NUNTIUS_NKEY` (seed string, e.g. `SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXV7JVDNABA2QX96`)
4. **No auth** — Default for local development

For TLS, set `NUNTIUS_TLS_CERT` and `NUNTIUS_TLS_KEY` together. The NATS URL should use `tls://` scheme.

### Multi-Instance Identity

Each running nuntius process is an independent Claude Code instance. Set `NUNTIUS_INSTANCE_ID` to give it a stable, human-readable name:

```bash
NUNTIUS_INSTANCE_ID=worker-1
NUNTIUS_INSTANCE_ID=main
NUNTIUS_INSTANCE_ID=review-bot
```

If not set, a random 8-character identifier is generated from a UUID at each startup. This means restart = new identity, which may break services that target this instance by ID.

**Recommendation:** Always set `NUNTIUS_INSTANCE_ID` in production configurations.

---

## Tools Reference

All tools return a JSON string as their content. On success, the JSON represents the result. On failure, `isError` is `true` and the content is an error message string.

### Core Messaging

#### `nats_publish`

Publish a message to a NATS subject. Fire-and-forget — no acknowledgement or reply is expected.

**Input:**

```json
{
  "subject": "events.user.created",
  "payload": "{\"user_id\": 42, \"email\": \"user@example.com\"}",
  "reply_to": "my.inbox.reply"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `subject` | string | yes | The NATS subject to publish to. May use dot notation. |
| `payload` | string | yes | The message body. Use JSON strings for structured data. |
| `reply_to` | string | no | Optional reply subject. Recipients can publish their response here. |

**Output (success):**
```json
{"ok": true}
```

**Use cases:**
- Sending events to subscribers (Animus, other agents, dashboards)
- One-way notifications
- Triggering side effects in external services

---

#### `nats_request`

Publish to a subject and block until a single reply arrives. Implements NATS request-reply: nuntius creates a private inbox subject and the recipient publishes their response there.

**Input:**

```json
{
  "subject": "services.lookup.user",
  "payload": "{\"user_id\": 42}",
  "timeout_ms": 3000
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `subject` | string | yes | The NATS subject to send the request to. |
| `payload` | string | yes | The request body. |
| `timeout_ms` | integer | no | How long to wait for a reply, in milliseconds. Default: `NUNTIUS_REQUEST_TIMEOUT_MS` (5000). |

**Output (success):**
```json
{
  "subject": "_INBOX.abc123",
  "payload": "{\"name\": \"Alice\", \"role\": \"admin\"}"
}
```

Binary reply payloads are base64-encoded.

**Output (timeout/no responder):**
```
error: no responder available for request — or — timed out after 3000ms
```

**Use cases:**
- Synchronous RPC to backend services
- Query operations against other agents
- Health checks that require a response

---

#### `nats_subscribe`

Subscribe to a NATS subject. All incoming messages on this subject are automatically delivered to Claude's context as `<channel>` notifications.

**Input:**

```json
{
  "subject": "animus.out.>",
  "queue_group": "claude-workers"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `subject` | string | yes | The subject to subscribe to. Supports wildcards: `*` matches a single token, `>` matches all remaining tokens. |
| `queue_group` | string | no | Optional queue group name. When set, only one subscriber in the group receives each message (load balancing). |

**Output (success):**
```json
{
  "subscription_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "subject": "animus.out.>"
}
```

Save the `subscription_id` if you need to unsubscribe later.

**Wildcard patterns:**

| Pattern | Matches | Does Not Match |
|---|---|---|
| `events.*` | `events.created`, `events.deleted` | `events.user.created` |
| `events.>` | `events.created`, `events.user.created`, `events.a.b.c` | `other.events` |
| `>` | Everything | — |

**Use cases:**
- Listening for tasks or directives from Animus
- Watching for system events
- Subscribing to a work queue with a group

---

#### `nats_unsubscribe`

Cancel an active subscription by its ID.

**Input:**

```json
{
  "subscription_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `subscription_id` | string | yes | The ID returned by `nats_subscribe`. |

**Output (success):**
```json
{"ok": true}
```

**Output (not found):**
```
error: subscription not found
```

---

### JetStream (Persistent Streams)

JetStream is NATS's persistence layer. Messages published to a stream are stored durably and can be replayed, consumed at any pace, and retained for a configurable duration. Requires NATS server started with `-js` or `jetstream: {}` in config.

#### `js_publish`

Publish a message to a JetStream stream. Unlike `nats_publish`, this waits for the server to acknowledge durability.

**Input:**

```json
{
  "subject": "logs.app.error",
  "payload": "{\"level\": \"error\", \"message\": \"disk full\"}",
  "msg_id": "unique-dedup-key-123"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `subject` | string | yes | Must match one of the stream's configured subjects. |
| `payload` | string | yes | The message body. |
| `msg_id` | string | no | Deduplication ID. If a message with this ID was recently published to the same stream, the server will mark it as a duplicate and not store it again. Useful for exactly-once semantics. |

**Output (success):**
```json
{
  "stream": "LOGS",
  "seq": 42,
  "duplicate": false
}
```

**Use cases:**
- Appending to an audit log
- Publishing tasks that must not be lost
- Exactly-once delivery with dedup IDs

---

#### `js_stream_create`

Create a new JetStream stream. A stream captures messages on one or more subjects and stores them durably.

**Input:**

```json
{
  "name": "TASKS",
  "subjects": ["work.tasks.>"],
  "max_msgs": 10000,
  "max_bytes": 104857600,
  "max_age_secs": 86400
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Stream name. Must not contain spaces, tabs, or periods. |
| `subjects` | array of strings | yes | NATS subjects this stream captures. Supports wildcards. |
| `max_msgs` | integer | no | Maximum number of messages to retain. Oldest are discarded when limit is hit. |
| `max_bytes` | integer | no | Maximum total storage in bytes. |
| `max_age_secs` | integer | no | Maximum age of retained messages in seconds. Messages older than this are discarded. |

**Output (success):**
```json
{
  "name": "TASKS",
  "subjects": ["work.tasks.>"]
}
```

**Output (already exists):**
If the stream already exists with identical configuration, the call succeeds silently (idempotent).

---

#### `js_stream_info`

Get current configuration and statistics for a JetStream stream.

**Input:**

```json
{
  "name": "TASKS"
}
```

**Output (success):**
```json
{
  "name": "TASKS",
  "subjects": ["work.tasks.>"],
  "messages": 128,
  "bytes": 40960,
  "max_age_nanos": 86400000000000,
  "max_age_secs": 86400
}
```

**Use cases:**
- Monitoring stream backlog before claiming work
- Verifying stream configuration
- Debugging message counts

---

#### `js_stream_delete`

Permanently delete a JetStream stream and all its stored messages. This is irreversible.

**Input:**

```json
{
  "name": "TASKS"
}
```

**Output (success):**
```json
{"ok": true}
```

---

#### `js_consume`

Pull messages from a JetStream stream. Creates a consumer (ephemeral or durable) and fetches up to `batch` messages.

**Input:**

```json
{
  "stream": "TASKS",
  "consumer_name": "claude-worker-1",
  "batch": 5,
  "timeout_ms": 3000
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `stream` | string | yes | The stream to consume from. |
| `consumer_name` | string | no | Durable consumer name. Messages acknowledged to this consumer will not be redelivered. If omitted, an ephemeral consumer is created. |
| `batch` | integer | no | Maximum number of messages to return in one call. Default: 1. |
| `timeout_ms` | integer | no | How long to wait if no messages are available, in milliseconds. Default: 5000. |

**Output (success, messages available):**
```json
[
  {
    "subject": "work.tasks.image-resize",
    "payload": "{\"file\": \"photo.jpg\", \"width\": 800}",
    "seq": 17,
    "headers": {}
  }
]
```

**Output (no messages / timeout):**
```json
[]
```

**Durable consumers:** A named `consumer_name` creates a durable consumer that remembers its position. If Claude Code restarts, it can resume from where it left off by using the same name. Ephemeral consumers (no name) start from the latest message each time.

---

### KV Store

JetStream KV provides a distributed key-value store backed by a JetStream stream. Keys can be created, read, overwritten, deleted, and listed. The store handles TTL (time-to-live) expiration natively if configured.

Buckets are created automatically on first use by any `kv_*` operation.

#### `kv_put`

Store a value under a key in a bucket. Overwrites any existing value. Returns the revision number, which increments on each write.

**Input:**

```json
{
  "bucket": "session-state",
  "key": "user:42:preferences",
  "value": "{\"theme\": \"dark\", \"lang\": \"en\"}"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `bucket` | string | yes | KV bucket name. Created automatically if it does not exist. |
| `key` | string | yes | The key to write. May use dots for namespacing (e.g. `user.42.prefs`). |
| `value` | string | yes | The value to store. Any string, including JSON. |

**Output (success):**
```json
{"revision": 3}
```

---

#### `kv_get`

Retrieve a value by key.

**Input:**

```json
{
  "bucket": "session-state",
  "key": "user:42:preferences"
}
```

**Output (success):**
```json
{
  "key": "user:42:preferences",
  "value": "{\"theme\": \"dark\", \"lang\": \"en\"}",
  "revision": 3
}
```

Binary values are base64-encoded.

**Output (not found):**
```
error: key not found
```

---

#### `kv_delete`

Delete a key from a bucket.

**Input:**

```json
{
  "bucket": "session-state",
  "key": "user:42:preferences"
}
```

**Output (success):**
```json
{"ok": true}
```

---

#### `kv_keys`

List all keys in a bucket, with optional prefix filtering.

**Input:**

```json
{
  "bucket": "session-state",
  "prefix": "user:42"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `bucket` | string | yes | KV bucket name. |
| `prefix` | string | no | Only return keys that start with this string. |

**Output (success):**
```json
{
  "keys": [
    "user:42:preferences",
    "user:42:session-token"
  ]
}
```

---

### Agent Coordination

These tools implement a multi-agent service discovery and coordination layer on top of NATS KV. The underlying bucket is `agents-registry` with a 5-minute TTL. Instances must heartbeat (via the automatic background task) to stay registered.

#### `agent_announce`

Register an agent in the shared registry. This is called automatically at startup by nuntius with the instance's own ID and capabilities `["claude-code", "mcp"]`. You can also call it manually to register an agent with custom capabilities.

**Input:**

```json
{
  "agent_id": "my-analysis-bot",
  "capabilities": ["code-analysis", "refactoring", "test-generation"],
  "metadata": {
    "version": "1.2.0",
    "language": "rust"
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `agent_id` | string | yes | Unique identifier for this agent. Use the instance ID for Claude Code instances. |
| `capabilities` | array of strings | yes | List of capability strings this agent supports. Used for filtering in `agent_discover`. |
| `metadata` | object | no | Arbitrary key-value metadata. |

**Output (success):**
```json
{"ok": true}
```

**Mechanism:** Publishes to `agents.registry.announce` (for subscribers) and writes to `agents-registry` KV bucket with the record including `last_seen` timestamp.

---

#### `agent_discover`

Query the registry for active agents, optionally filtering by capability.

**Input:**

```json
{
  "capability": "claude-code"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `capability` | string | no | If provided, only return agents whose `capabilities` array includes this string. |

**Output (success):**
```json
[
  {
    "agent_id": "main",
    "capabilities": ["claude-code", "mcp"],
    "metadata": {
      "type": "claude-code",
      "nuntius_version": "0.1.0"
    },
    "last_seen": "2024-11-05T12:34:56Z"
  },
  {
    "agent_id": "worker-1",
    "capabilities": ["claude-code", "mcp"],
    "metadata": {
      "type": "claude-code",
      "nuntius_version": "0.1.0"
    },
    "last_seen": "2024-11-05T12:34:55Z"
  }
]
```

**Note on staleness:** Entries expire after 5 minutes if the instance stops heartbeating. An entry being present means the instance announced itself within the last 5 minutes — it does not guarantee the process is still alive. Check `last_seen` for recency.

---

#### `agent_claim`

Claim a single task message from a NATS subject using a queue group. Only one agent in the pool will receive each message, providing load-balanced work distribution.

**Input:**

```json
{
  "subject": "work.tasks.image-resize",
  "timeout_ms": 2000
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `subject` | string | yes | The NATS subject to claim from. |
| `timeout_ms` | integer | no | How long to wait for a message, in milliseconds. Default: 1000. |

**Output (message claimed):**
```json
{
  "claimed": true,
  "task": {
    "file": "photo.jpg",
    "width": 800
  }
}
```

**Output (nothing available):**
```json
{"claimed": false}
```

**Queue group:** All calls to `agent_claim` on the same subject share the queue group `nuntius.claim`. NATS guarantees exactly one subscriber in the group receives each message, making this safe for concurrent Claude Code instances pulling from the same subject.

---

#### `request_permission`

Send a permission request to an external supervisor (e.g., Animus) and block until it approves or denies the action. Use this before executing risky, irreversible, or sensitive operations.

**Input:**

```json
{
  "action": "shell_exec",
  "details": "Run `git push --force origin main` to overwrite the remote branch",
  "timeout_ms": 120000
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `action` | string | yes | Short action type. Suggested values: `shell_exec`, `file_delete`, `network_request`, `write_file`, `git_push`, `deploy`. |
| `details` | string | yes | Full description of what will be executed and why it is necessary. Be specific — the supervisor uses this to make the decision. |
| `timeout_ms` | integer | no | How long to wait for the supervisor's response in milliseconds. Default: 30000 (30 sec). Increase for actions requiring human input (e.g., 120000). |

**Output (approved):**
```json
{
  "request_id": "a1b2c3d4",
  "approved": true,
  "reason": "safe operation, confirmed by user"
}
```

**Output (denied):**
```
error: Permission denied: this would overwrite production data
```

**Output (timeout):**
```
error: Permission request timed out after 120000ms — Animus did not respond
```

**Output (no supervisor):**
```
error: No responder for permission request — is Animus running?
```

**Mechanism:** Sends a NATS request to `animus.in.permission_request` with the payload:
```json
{
  "request_id": "a1b2c3d4",
  "from": "main",
  "action": "shell_exec",
  "details": "...",
  "timestamp": "2024-11-05T12:34:56Z"
}
```

The supervisor must respond with:
```json
{"approved": true, "reason": "optional reason string"}
```
or:
```json
{"approved": false, "reason": "why it was denied"}
```

---

## Inbound Messages (NATS → Claude)

When a message arrives on any active subscription, nuntius delivers it to Claude Code as an MCP notification using the method `notifications/claude/channel`. Claude Code renders this as a `<channel>` tag in context.

**Notification structure:**
```json
{
  "jsonrpc": "2.0",
  "method": "notifications/claude/channel",
  "params": {
    "content": "the message payload as a string",
    "meta": {
      "subject": "animus.out.directive",
      "ts": "2024-11-05T12:34:56.789Z",
      "reply_to": "_INBOX.abc123"
    }
  }
}
```

- `content` — UTF-8 payloads are delivered as strings. Binary payloads are base64-encoded.
- `meta.subject` — The NATS subject the message arrived on.
- `meta.ts` — RFC3339 timestamp of when nuntius received the message.
- `meta.reply_to` — The reply inbox subject, if the sender used request-reply. Use `nats_publish` with this subject to respond.

**Auto-subscribed subjects:**
1. `claude.{NUNTIUS_INSTANCE_ID}.in.>` — Instance-targeted messages (subscribed at startup, always active)
2. Any subjects in `NUNTIUS_STARTUP_SUBS` — Subscribed at startup before the first tool call

---

## Subject Naming Conventions

Nuntius uses and recommends the following subject conventions:

| Pattern | Direction | Description |
|---|---|---|
| `claude.{id}.in.{topic}` | Animus → Claude | Targeted directive to a specific Claude instance |
| `claude.{id}.out.{topic}` | Claude → Animus | Response or event from a specific Claude instance |
| `claude.broadcast.in.{topic}` | Animus → All Claude | Broadcast to all running Claude instances |
| `agents.registry.announce` | Any → Subscribers | Agent registry announcements |
| `animus.in.permission_request` | Claude → Animus | Permission gate requests (request-reply) |
| `animus.out.>` | Animus → Claude | All output from Animus (recommended startup sub) |
| `work.tasks.{type}` | Producer → Worker pool | Work items for `agent_claim` |

**NATS subject rules:**
- Tokens are separated by `.`
- `*` matches exactly one token
- `>` matches one or more tokens and must be the last token
- No spaces; valid characters are letters, digits, `-`, `_`, `/`

---

## Multi-Instance Architecture

Multiple Claude Code instances can run simultaneously, each with its own nuntius process. They coordinate through the shared NATS server using the agent registry and instance-specific subjects.

```
Claude "main"          Claude "worker-1"        Claude "review-bot"
   │                       │                         │
   │  claude.main.in.>     │  claude.worker-1.in.>  │  claude.review-bot.in.>
   └───────────┬───────────┘──────────┬──────────────┘─────────┬────────────
               │                      │                         │
               └──────────────────────┼─────────────────────────┘
                                      │
                                NATS Server
                                      │
                               agents-registry (KV)
                               ┌─────────────────────┐
                               │ main       → {...}  │
                               │ worker-1   → {...}  │
                               │ review-bot → {...}  │
                               └─────────────────────┘
```

### Agent Registry

At startup, every nuntius instance writes to the `agents-registry` KV bucket:

```json
{
  "agent_id": "main",
  "capabilities": ["claude-code", "mcp"],
  "metadata": {
    "type": "claude-code",
    "nuntius_version": "0.1.0"
  },
  "last_seen": "2024-11-05T12:34:56Z"
}
```

Any service can call `agent_discover(capability: "claude-code")` to find all active Claude Code instances, including their IDs and `last_seen` timestamps.

### Heartbeat and TTL

- **TTL:** Registry entries expire after **5 minutes** if not refreshed.
- **Heartbeat interval:** Nuntius refreshes its own entry every **2 minutes** (120 seconds) in a background task.
- This means an entry disappears no more than 5 minutes after the instance shuts down (without explicit deregistration).

If you stop nuntius and want immediate cleanup, the entry will naturally expire within 5 minutes.

### Targeting a Specific Instance

To send a directive to a specific Claude Code instance, publish to its inbound channel:

```
nats_publish(subject: "claude.worker-1.in.task", payload: "{...}")
```

That instance's background subscription task picks it up and delivers it as a `<channel>` notification.

### Load-Balanced Worker Pools

To distribute work across multiple instances, all instances can call `agent_claim` on the same subject. The `nuntius.claim` queue group ensures each message goes to exactly one instance:

```
nats_publish(subject: "work.tasks.render", payload: "{...}")   ← producer
agent_claim(subject: "work.tasks.render")                       ← any available worker claims it
```

---

## Permission Gates

The `request_permission` tool creates a synchronous approval checkpoint. Before Claude executes a destructive or irreversible operation, it sends a structured request to Animus (or any other supervisor listening on `animus.in.permission_request`) and waits for an explicit `{approved: true}` response.

**Typical flow:**
1. Claude is about to run `git push --force`
2. Claude calls `request_permission(action: "git_push", details: "force push to main — this will overwrite remote history")`
3. Nuntius sends a NATS request to Animus
4. Animus notifies the user via Telegram (or another channel)
5. The user approves or denies
6. Animus publishes `{approved: true, reason: "user confirmed"}` to the reply inbox
7. `request_permission` returns `{approved: true}` to Claude
8. Claude proceeds with the push

**When to use it:**
- Shell commands (`shell_exec`)
- File deletions or overwrites (`file_delete`, `write_file`)
- Git operations that can't be undone (`git_push`, `git_reset`)
- Network requests to external systems (`network_request`)
- Deployments or infrastructure changes (`deploy`)

**Timeout guidance:**
- Use `timeout_ms: 30000` (default) for automated decisions by Animus
- Use `timeout_ms: 120000` or more when a human needs to respond via a mobile notification
- If Animus is not running, the request will fail with a "no responder" error rather than silently approving

---

## Startup Behavior

When nuntius starts, it executes the following sequence before entering the stdin read loop:

1. **Configure logging** — `RUST_LOG` → stderr via `tracing-subscriber`
2. **Load config** — Reads all `NUNTIUS_*` environment variables
3. **Connect to NATS** — Using the configured auth method. Exits with code 1 if connection fails.
4. **Subscribe to startup subjects** — Any subjects in `NUNTIUS_STARTUP_SUBS` are subscribed immediately
5. **Subscribe to instance channel** — `claude.{instance_id}.in.>` is subscribed automatically
6. **Announce to agent registry** — Calls `agent_announce` with `["claude-code", "mcp"]` capabilities
7. **Start heartbeat task** — Background Tokio task refreshes registry entry every 120 seconds
8. **Enter MCP read loop** — Processes JSON-RPC requests from stdin

---

## Logging and Observability

All logs go to **stderr**. Stdout is reserved exclusively for the MCP protocol channel.

Enable logging with the `RUST_LOG` environment variable:

```bash
# Info level for all nuntius code
RUST_LOG=info

# Debug level for nuntius-core only
RUST_LOG=nuntius_core=debug

# Trace everything (very verbose)
RUST_LOG=trace

# Specific module
RUST_LOG=nuntius_core::tools::agent=debug
```

**What gets logged:**
- `INFO` — Startup events, successful announces, subscription setup, heartbeat refreshes
- `WARN` — Failed operations that are non-fatal (announce failure, heartbeat failure, broken stdout pipe)
- `DEBUG` — Heartbeat ticks, subscription routing
- `TRACE` — Raw message payloads (only when explicitly enabled)

---

## Testing

Tests use a real `nats-server` subprocess for full integration coverage. Each test spawns its own isolated server instance on a random port with a unique temp directory to prevent state leakage.

```bash
# Run all unit and integration tests
cargo test

# Run only unit tests (no integration binary build)
cargo test -p nuntius-core

# Run only integration tests
cargo test -p nuntius

# Run a specific test with output
cargo test -p nuntius-core test_kv_ttl_on_creation -- --nocapture

# Run with logging visible
RUST_LOG=nuntius_core=debug cargo test -p nuntius-core -- --nocapture
```

**Prerequisites for tests:**
- `nats-server` must be in `PATH` with JetStream support (`nats-server -js`)

Install: `brew install nats-server`

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
