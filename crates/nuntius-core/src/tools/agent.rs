use crate::client::NatsBridge;
use super::{Tool, ToolResult};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::Value;
use std::time::Duration;

pub struct AgentAnnounce;
pub struct AgentDiscover;
pub struct AgentClaim;
pub struct RequestPermission;

const AGENTS_KV_BUCKET: &str = "agents-registry";
/// TTL for registry entries. Instances must heartbeat within this window or be evicted.
const AGENTS_KV_TTL: Duration = Duration::from_secs(300); // 5 minutes
const AGENTS_ANNOUNCE_SUBJECT: &str = "agents.registry.announce";
const CLAIM_QUEUE_GROUP: &str = "nuntius.claim";
const PERMISSION_REQUEST_SUBJECT: &str = "animus.in.permission_request";

/// Build the agents-registry record, injecting top-level `role`/`display` from the
/// `NUNTIUS_ROLE` / `NUNTIUS_DISPLAY` env vars when they are set and non-empty.
///
/// This is the SINGLE source of truth for the registry record shape. Both KV writers
/// — the `agent_announce` tool and the heartbeat `refresh_instance_registration` —
/// build through here, so the heartbeat can never clobber the role/display the tool
/// wrote (and vice versa). nuntius is the only writer of this bucket; launch scripts
/// must NOT also write it.
///
/// Backward-compatible: with neither env var set (or set to empty), the record is
/// byte-for-byte what it was before — `agent_id` / `capabilities` / `metadata` /
/// `last_seen` only, no `role`/`display` keys.
fn build_registry_record(
    agent_id: &str,
    capabilities: Value,
    metadata: Value,
    last_seen: &str,
) -> Value {
    let mut record = serde_json::json!({
        "agent_id": agent_id,
        "capabilities": capabilities,
        "metadata": metadata,
        "last_seen": last_seen,
    });
    // Treat an unset OR empty env var as "absent" so `NUNTIUS_ROLE=""` keeps today's
    // behavior rather than emitting a blank role.
    if let Some(role) = std::env::var("NUNTIUS_ROLE").ok().filter(|s| !s.is_empty()) {
        record["role"] = Value::String(role);
    }
    if let Some(display) = std::env::var("NUNTIUS_DISPLAY").ok().filter(|s| !s.is_empty()) {
        record["display"] = Value::String(display);
    }
    record
}

/// Refresh an agent's registry entry. Called by the heartbeat task in main.rs.
/// Uses a raw NATS client so it can run in a background tokio task without
/// needing a full NatsBridge (which holds a notification sender).
pub async fn refresh_instance_registration(
    client: &async_nats::Client,
    agent_id: &str,
    nuntius_version: &str,
) -> Result<(), String> {
    let record = build_registry_record(
        agent_id,
        serde_json::json!(["claude-code", "mcp"]),
        serde_json::json!({ "type": "claude-code", "nuntius_version": nuntius_version }),
        &Utc::now().to_rfc3339(),
    );
    let record_str = serde_json::to_string(&record).map_err(|e| e.to_string())?;
    let bytes: Vec<u8> = record_str.as_bytes().to_vec();

    // Best-effort announce publish (fire-and-forget)
    let _ = client.publish(AGENTS_ANNOUNCE_SUBJECT, bytes.clone().into()).await;

    // Update KV entry to reset the TTL clock
    let js = jetstream::new(client.clone());
    match js.get_key_value(AGENTS_KV_BUCKET).await {
        Ok(kv) => kv.put(agent_id, bytes.into()).await.map_err(|e| e.to_string()).map(|_| ()),
        Err(e) => Err(e.to_string()),
    }
}

// Same try-then-create pattern as kv.rs — get_or_create_key_value doesn't exist in async-nats 0.38
async fn get_or_create_agents_kv(bridge: &NatsBridge) -> Result<jetstream::kv::Store, String> {
    let js = async_nats::jetstream::new(bridge.client().clone());
    match js.get_key_value(AGENTS_KV_BUCKET).await {
        Ok(store) => {
            // If the bucket exists but has the wrong TTL (e.g. created before TTL was added),
            // update the underlying stream config to apply the correct max_age.
            let current_max_age = store.stream.cached_info().config.max_age;
            if current_max_age != AGENTS_KV_TTL {
                let mut updated_config = store.stream.cached_info().config.clone();
                updated_config.max_age = AGENTS_KV_TTL;
                if let Err(e) = js.update_stream(updated_config).await {
                    tracing::warn!("agents-registry: failed to update TTL from {}s to {}s: {e}",
                        current_max_age.as_secs(), AGENTS_KV_TTL.as_secs());
                } else {
                    tracing::info!("agents-registry: updated TTL to {}s", AGENTS_KV_TTL.as_secs());
                }
            }
            return Ok(store);
        }
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if !msg.contains("not found") && !msg.contains("stream not found") && !msg.contains("bucket not found") {
                return Err(e.to_string());
            }
        }
    }
    match js.create_key_value(jetstream::kv::Config {
        bucket: AGENTS_KV_BUCKET.to_string(),
        max_age: AGENTS_KV_TTL,
        ..Default::default()
    }).await {
        Ok(store) => Ok(store),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if msg.contains("already exists") || msg.contains("existing configuration") {
                js.get_key_value(AGENTS_KV_BUCKET).await.map_err(|e| e.to_string())
            } else {
                Err(e.to_string())
            }
        }
    }
}

// ── agent_announce ─────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Tool for AgentAnnounce {
    fn name(&self) -> &str { "agent_announce" }
    fn description(&self) -> &str { "Announce an agent's presence and capabilities to the registry." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["agent_id", "capabilities"],
            "properties": {
                "agent_id": { "type": "string", "description": "Unique identifier for this agent" },
                "capabilities": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "List of capability strings this agent supports"
                },
                "metadata": {
                    "type": "object",
                    "description": "Optional arbitrary metadata for this agent"
                },
                "role": {
                    "type": "string",
                    "description": "Optional role; overrides the NUNTIUS_ROLE env var. Emitted as a top-level `role` field in the registry record."
                },
                "display": {
                    "type": "string",
                    "description": "Optional display name; overrides the NUNTIUS_DISPLAY env var. Emitted as a top-level `display` field in the registry record."
                }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let agent_id = match params["agent_id"].as_str() {
            Some(s) => s.to_string(),
            None => return ToolResult::err("missing agent_id"),
        };

        let capabilities = match params["capabilities"].as_array() {
            Some(arr) => arr.clone(),
            None => return ToolResult::err("missing capabilities"),
        };

        let metadata = params["metadata"].clone();
        let metadata = if metadata.is_null() {
            serde_json::json!({})
        } else {
            metadata
        };

        let last_seen = Utc::now().to_rfc3339();

        // Build through the shared helper so the env-derived role/display match the
        // heartbeat path exactly (no clobber across writers).
        let mut record = build_registry_record(
            &agent_id,
            Value::Array(capabilities),
            metadata,
            &last_seen,
        );
        // Explicit role/display params override the env-derived values — useful for
        // programmatic callers and for tests that must not mutate process-global env.
        if let Some(role) = params["role"].as_str().filter(|s| !s.is_empty()) {
            record["role"] = Value::String(role.to_string());
        }
        if let Some(display) = params["display"].as_str().filter(|s| !s.is_empty()) {
            record["display"] = Value::String(display.to_string());
        }

        let record_bytes = match serde_json::to_string(&record) {
            Ok(s) => s,
            Err(e) => return ToolResult::err(format!("serialize error: {e}")),
        };

        // Publish to announce subject
        if let Err(e) = bridge.client().publish(
            AGENTS_ANNOUNCE_SUBJECT,
            record_bytes.as_bytes().to_vec().into(),
        ).await {
            return ToolResult::err(format!("publish error: {e}"));
        }

        // Write to KV bucket
        let kv = match get_or_create_agents_kv(bridge).await {
            Ok(kv) => kv,
            Err(e) => return ToolResult::err(format!("kv error: {e}")),
        };

        match kv.put(&agent_id, record_bytes.as_bytes().to_vec().into()).await {
            Ok(_) => ToolResult::ok(serde_json::json!({ "ok": true })),
            Err(e) => ToolResult::err(format!("kv put error: {e}")),
        }
    }
}

// ── agent_discover ─────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Tool for AgentDiscover {
    fn name(&self) -> &str { "agent_discover" }
    fn description(&self) -> &str { "Discover agents in the registry, optionally filtered by capability." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "capability": {
                    "type": "string",
                    "description": "Optional capability filter — only return agents that have this capability"
                }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let capability_filter = params["capability"].as_str();

        let kv = match get_or_create_agents_kv(bridge).await {
            Ok(kv) => kv,
            Err(e) => return ToolResult::err(format!("kv error: {e}")),
        };

        // Get all keys
        let keys_stream = match kv.keys().await {
            Ok(s) => s,
            Err(e) => return ToolResult::err(format!("keys error: {e}")),
        };

        use futures::StreamExt;
        let keys: Vec<String> = {
            let mut stream = keys_stream;
            let mut result = Vec::new();
            while let Some(key) = stream.next().await {
                match key {
                    Ok(k) => result.push(k),
                    Err(e) => return ToolResult::err(format!("key stream error: {e}")),
                }
            }
            result
        };

        let mut agents = Vec::new();

        for key in keys {
            match kv.entry(&key).await {
                Ok(Some(entry)) if entry.operation == jetstream::kv::Operation::Put => {
                    let record: Value = match serde_json::from_slice(&entry.value) {
                        Ok(v) => v,
                        Err(_) => continue, // skip malformed entries
                    };

                    // Apply capability filter if provided
                    if let Some(cap) = capability_filter {
                        if let Some(caps) = record["capabilities"].as_array() {
                            let has_cap = caps.iter().any(|c| c.as_str() == Some(cap));
                            if !has_cap {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }

                    agents.push(record);
                }
                Ok(_) => {} // deleted or purged, skip
                Err(e) => return ToolResult::err(format!("entry error for {key}: {e}")),
            }
        }

        ToolResult::ok(serde_json::json!(agents))
    }
}

// ── agent_claim ────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Tool for AgentClaim {
    fn name(&self) -> &str { "agent_claim" }
    fn description(&self) -> &str { "Claim a task from a subject using queue-group subscription." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["subject"],
            "properties": {
                "subject": { "type": "string", "description": "NATS subject to subscribe to for tasks" },
                "timeout_ms": {
                    "type": "integer",
                    "description": "How long to wait for a message in milliseconds (default 1000)"
                }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let subject = match params["subject"].as_str() {
            Some(s) => s.to_string(),
            None => return ToolResult::err("missing subject"),
        };
        let timeout_ms = params["timeout_ms"].as_u64().unwrap_or(1000);

        let mut subscriber = match bridge.client()
            .queue_subscribe(subject.clone(), CLAIM_QUEUE_GROUP.to_string())
            .await
        {
            Ok(sub) => sub,
            Err(e) => return ToolResult::err(format!("subscribe error: {e}")),
        };

        use futures::StreamExt;
        let result = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            subscriber.next(),
        ).await;

        // Unsubscribe regardless of outcome
        let _ = subscriber.unsubscribe().await;

        match result {
            Ok(Some(msg)) => {
                let task: Value = match serde_json::from_slice(&msg.payload) {
                    Ok(v) => v,
                    Err(_) => {
                        // Return raw string if not valid JSON
                        let raw = String::from_utf8_lossy(&msg.payload).to_string();
                        serde_json::json!(raw)
                    }
                };
                ToolResult::ok(serde_json::json!({ "claimed": true, "task": task }))
            }
            Ok(None) => {
                // Subscriber closed with no message
                ToolResult::ok(serde_json::json!({ "claimed": false }))
            }
            Err(_) => {
                // Timeout
                ToolResult::ok(serde_json::json!({ "claimed": false }))
            }
        }
    }
}

// ── request_permission ─────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Tool for RequestPermission {
    fn name(&self) -> &str { "request_permission" }

    fn description(&self) -> &str {
        "Send a permission request to Animus and wait for approval before executing a \
         risky or irreversible operation. Use before shell commands, file deletions, \
         network requests, or anything requiring human or AI oversight. \
         Animus evaluates the request and may consult the user (e.g. via Telegram). \
         Returns {approved: true} on success or an error containing the denial reason."
    }

    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["action", "details"],
            "properties": {
                "action": {
                    "type": "string",
                    "description": "Short action type: shell_exec, file_delete, network_request, write_file, etc."
                },
                "details": {
                    "type": "string",
                    "description": "Full description of what will be executed and why it is necessary"
                },
                "timeout_ms": {
                    "type": "integer",
                    "description": "How long to wait for Animus to respond in ms (default: 30000). \
                                    Increase for requests that require human input (e.g. 120000)."
                }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let action = match params["action"].as_str() {
            Some(s) => s.to_string(),
            None => return ToolResult::err("missing required field: action"),
        };
        let details = match params["details"].as_str() {
            Some(s) => s.to_string(),
            None => return ToolResult::err("missing required field: details"),
        };
        let timeout_ms = params["timeout_ms"].as_u64().unwrap_or(30_000);

        // Include the instance ID so Animus knows which session is asking
        let instance_id = std::env::var("NUNTIUS_INSTANCE_ID")
            .unwrap_or_else(|_| "unknown".to_string());

        let request_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let payload = serde_json::json!({
            "request_id": request_id,
            "from": instance_id,
            "action": action,
            "details": details,
            "timestamp": Utc::now().to_rfc3339(),
        });
        let payload_str = match serde_json::to_string(&payload) {
            Ok(s) => s,
            Err(e) => return ToolResult::err(format!("serialize error: {e}")),
        };

        let req_future = bridge.client()
            .request(PERMISSION_REQUEST_SUBJECT, payload_str.as_bytes().to_vec().into());

        let response_msg = match tokio::time::timeout(Duration::from_millis(timeout_ms), req_future).await {
            Ok(Ok(msg)) => msg,
            Ok(Err(e)) => return ToolResult::err(
                format!("No responder for permission request — is Animus running? ({e})")
            ),
            Err(_) => return ToolResult::err(
                format!("Permission request timed out after {timeout_ms}ms — Animus did not respond")
            ),
        };

        let body = std::str::from_utf8(&response_msg.payload)
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "invalid response encoding".to_string());

        // Parse structured response: {approved: bool, reason?: string}
        match serde_json::from_str::<Value>(&body) {
            Ok(v) => {
                let approved = v["approved"].as_bool().unwrap_or(false);
                let reason = v["reason"].as_str().unwrap_or(if approved { "approved" } else { "denied" });
                if approved {
                    ToolResult::ok(serde_json::json!({
                        "request_id": request_id,
                        "approved": true,
                        "reason": reason,
                    }))
                } else {
                    ToolResult::err(format!("Permission denied: {reason}"))
                }
            }
            Err(_) => {
                // Animus sent a non-JSON response — treat as approved if it looks positive
                if body.to_lowercase().contains("approved") || body.to_lowercase().contains("yes") {
                    ToolResult::ok(serde_json::json!({ "request_id": request_id, "approved": true, "raw": body }))
                } else {
                    ToolResult::err(format!("Permission denied or unrecognized response: {body}"))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, NatsBridge};
    use tokio::sync::mpsc;
    use std::net::TcpListener;
    use std::process::{Child, Command};

    struct NatsServer(Child);
    impl Drop for NatsServer {
        fn drop(&mut self) { let _ = self.0.kill(); let _ = self.0.wait(); }
    }

    async fn start_nats_js() -> (NatsServer, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        // Use a unique temp dir per test run to avoid KV state leaking across tests
        let store_dir = std::env::temp_dir().join(format!("nuntius-test-{port}"));
        let child = Command::new("nats-server")
            .args(["-p", &port.to_string(), "-js", "-sd", store_dir.to_str().unwrap()])
            .spawn().expect("nats-server not in PATH");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        (NatsServer(child), format!("nats://127.0.0.1:{port}"))
    }

    async fn make_bridge(url: &str) -> NatsBridge {
        let cfg = Config { nats_url: url.to_string(), ..Config::from_env() };
        let (tx, _) = mpsc::unbounded_channel();
        NatsBridge::connect(&cfg, tx).await.unwrap()
    }

    #[tokio::test]
    async fn test_announce_and_discover() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        // Announce agent1 with capabilities ["search", "summarize"]
        let ann1 = AgentAnnounce.execute(serde_json::json!({
            "agent_id": "agent-1",
            "capabilities": ["search", "summarize"],
            "metadata": { "version": "1.0" }
        }), &bridge).await;
        assert!(!ann1.is_error, "announce agent-1 failed: {}", ann1.content);
        let v1: Value = serde_json::from_str(&ann1.content).unwrap();
        assert_eq!(v1["ok"], true);

        // Announce agent2 with capabilities ["embed"]
        let ann2 = AgentAnnounce.execute(serde_json::json!({
            "agent_id": "agent-2",
            "capabilities": ["embed"],
            "metadata": {}
        }), &bridge).await;
        assert!(!ann2.is_error, "announce agent-2 failed: {}", ann2.content);

        // Discover all agents
        let disc_all = AgentDiscover.execute(serde_json::json!({}), &bridge).await;
        assert!(!disc_all.is_error, "discover all failed: {}", disc_all.content);
        let all: Vec<Value> = serde_json::from_str(&disc_all.content).unwrap();
        assert_eq!(all.len(), 2, "expected 2 agents, got {}: {}", all.len(), disc_all.content);

        // Discover by capability "search" — should return only agent-1
        let disc_search = AgentDiscover.execute(serde_json::json!({
            "capability": "search"
        }), &bridge).await;
        assert!(!disc_search.is_error, "discover search failed: {}", disc_search.content);
        let search_agents: Vec<Value> = serde_json::from_str(&disc_search.content).unwrap();
        assert_eq!(search_agents.len(), 1, "expected 1 agent with search capability");
        assert_eq!(search_agents[0]["agent_id"], "agent-1");

        // Discover by capability "embed" — should return only agent-2
        let disc_embed = AgentDiscover.execute(serde_json::json!({
            "capability": "embed"
        }), &bridge).await;
        assert!(!disc_embed.is_error, "discover embed failed: {}", disc_embed.content);
        let embed_agents: Vec<Value> = serde_json::from_str(&disc_embed.content).unwrap();
        assert_eq!(embed_agents.len(), 1);
        assert_eq!(embed_agents[0]["agent_id"], "agent-2");

        // Discover by capability that no agent has — should return empty
        let disc_none = AgentDiscover.execute(serde_json::json!({
            "capability": "nonexistent"
        }), &bridge).await;
        assert!(!disc_none.is_error, "discover nonexistent failed: {}", disc_none.content);
        let none_agents: Vec<Value> = serde_json::from_str(&disc_none.content).unwrap();
        assert_eq!(none_agents.len(), 0);

        // Verify record fields are present
        let rec = &search_agents[0];
        assert!(rec["agent_id"].is_string());
        assert!(rec["capabilities"].is_array());
        assert!(rec["metadata"].is_object());
        assert!(rec["last_seen"].is_string());
    }

    #[tokio::test]
    async fn test_claim() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        // Spawn a delayed publish so claim's subscriber is set up first
        let client = bridge.client().clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            client.publish("work.tasks", r#"{"task_id":"t1"}"#.into()).await.unwrap();
        });

        // Claim it (subscription created inside execute, then waits for message)
        let claim = AgentClaim.execute(serde_json::json!({
            "subject": "work.tasks",
            "timeout_ms": 2000
        }), &bridge).await;
        assert!(!claim.is_error, "{}", claim.content);
        let v: Value = serde_json::from_str(&claim.content).unwrap();
        assert_eq!(v["claimed"], true);
        assert!(v["task"].to_string().contains("t1"));

        // Second claim on empty queue should return claimed: false
        let claim2 = AgentClaim.execute(serde_json::json!({
            "subject": "work.tasks",
            "timeout_ms": 300
        }), &bridge).await;
        assert!(!claim2.is_error);
        let v2: Value = serde_json::from_str(&claim2.content).unwrap();
        assert_eq!(v2["claimed"], false);
    }

    #[tokio::test]
    async fn test_announce_missing_params() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        // Missing agent_id
        let r1 = AgentAnnounce.execute(serde_json::json!({
            "capabilities": ["search"]
        }), &bridge).await;
        assert!(r1.is_error, "expected error for missing agent_id");

        // Missing capabilities
        let r2 = AgentAnnounce.execute(serde_json::json!({
            "agent_id": "x"
        }), &bridge).await;
        assert!(r2.is_error, "expected error for missing capabilities");
    }

    // NUNTIUS_ROLE / NUNTIUS_DISPLAY are process-global env; serialize the sync
    // unit tests that mutate them so they don't observe each other's writes.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_build_registry_record_injects_env_role_display() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("NUNTIUS_ROLE", "messaging-substrate");
        std::env::set_var("NUNTIUS_DISPLAY", "Nuntius");
        let rec = build_registry_record(
            "a",
            serde_json::json!(["claude-code", "mcp"]),
            serde_json::json!({ "type": "claude-code" }),
            "2026-01-01T00:00:00Z",
        );
        assert_eq!(rec["role"], "messaging-substrate");
        assert_eq!(rec["display"], "Nuntius");
        assert_eq!(rec["agent_id"], "a");
        assert_eq!(rec["metadata"]["type"], "claude-code");
        std::env::remove_var("NUNTIUS_ROLE");
        std::env::remove_var("NUNTIUS_DISPLAY");
    }

    #[test]
    fn test_build_registry_record_absent_env_is_backward_compatible() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("NUNTIUS_ROLE");
        std::env::remove_var("NUNTIUS_DISPLAY");
        let rec = build_registry_record(
            "a",
            serde_json::json!(["claude-code", "mcp"]),
            serde_json::json!({ "k": "v" }),
            "2026-01-01T00:00:00Z",
        );
        assert!(rec.get("role").is_none(), "role must be absent: {rec}");
        assert!(rec.get("display").is_none(), "display must be absent: {rec}");
        // Byte-for-byte the legacy record shape.
        assert_eq!(rec, serde_json::json!({
            "agent_id": "a",
            "capabilities": ["claude-code", "mcp"],
            "metadata": { "k": "v" },
            "last_seen": "2026-01-01T00:00:00Z",
        }));
    }

    #[test]
    fn test_build_registry_record_empty_env_treated_as_absent() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("NUNTIUS_ROLE", "");
        std::env::set_var("NUNTIUS_DISPLAY", "");
        let rec = build_registry_record("a", serde_json::json!([]), serde_json::json!({}), "t");
        assert!(rec.get("role").is_none(), "empty NUNTIUS_ROLE => absent: {rec}");
        assert!(rec.get("display").is_none(), "empty NUNTIUS_DISPLAY => absent: {rec}");
        std::env::remove_var("NUNTIUS_ROLE");
        std::env::remove_var("NUNTIUS_DISPLAY");
    }

    #[tokio::test]
    async fn test_announce_role_display_via_params_roundtrip() {
        // Param path: no env mutation, so this is safe to run in parallel.
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        let ann = AgentAnnounce.execute(serde_json::json!({
            "agent_id": "role-agent",
            "capabilities": ["claude-code", "mcp"],
            "role": "messaging-substrate",
            "display": "Nuntius"
        }), &bridge).await;
        assert!(!ann.is_error, "announce failed: {}", ann.content);

        let disc = AgentDiscover.execute(serde_json::json!({}), &bridge).await;
        assert!(!disc.is_error, "discover failed: {}", disc.content);
        let agents: Vec<Value> = serde_json::from_str(&disc.content).unwrap();
        let rec = agents.iter().find(|a| a["agent_id"] == "role-agent")
            .expect("role-agent should be in registry");
        assert_eq!(rec["role"], "messaging-substrate");
        assert_eq!(rec["display"], "Nuntius");
        // Explicit params must win over env.
        let ann2 = AgentAnnounce.execute(serde_json::json!({
            "agent_id": "role-agent",
            "capabilities": ["claude-code", "mcp"],
            "role": "override-role"
        }), &bridge).await;
        assert!(!ann2.is_error, "re-announce failed: {}", ann2.content);
        let disc2 = AgentDiscover.execute(serde_json::json!({}), &bridge).await;
        let agents2: Vec<Value> = serde_json::from_str(&disc2.content).unwrap();
        let rec2 = agents2.iter().find(|a| a["agent_id"] == "role-agent").unwrap();
        assert_eq!(rec2["role"], "override-role");
    }

    #[tokio::test]
    async fn test_claim_missing_subject() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        let r = AgentClaim.execute(serde_json::json!({}), &bridge).await;
        assert!(r.is_error, "expected error for missing subject");
    }

    #[tokio::test]
    async fn test_kv_ttl_on_creation() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        // First announce creates the bucket — verify TTL is set
        let r = AgentAnnounce.execute(serde_json::json!({
            "agent_id": "ttl-test-agent",
            "capabilities": ["test"]
        }), &bridge).await;
        assert!(!r.is_error, "announce failed: {}", r.content);

        // Query the underlying stream to check max_age
        let js = async_nats::jetstream::new(bridge.client().clone());
        let mut stream = js.get_stream("KV_agents-registry").await
            .expect("KV stream should exist after announce");
        let info = stream.info().await.expect("stream info failed");
        let got_ttl = info.config.max_age;
        assert_eq!(
            got_ttl, AGENTS_KV_TTL,
            "max_age should be {} but got {} ({}ns)",
            AGENTS_KV_TTL.as_secs(), got_ttl.as_secs(), got_ttl.as_nanos()
        );

        // Delete the bucket via the raw stream delete (same as js_stream_delete tool)
        js.delete_stream("KV_agents-registry").await
            .expect("stream delete failed");

        // Re-announce — should recreate bucket with TTL
        let r2 = AgentAnnounce.execute(serde_json::json!({
            "agent_id": "ttl-test-agent-2",
            "capabilities": ["test"]
        }), &bridge).await;
        assert!(!r2.is_error, "re-announce failed: {}", r2.content);

        // Verify TTL is still set after recreate
        let mut stream2 = js.get_stream("KV_agents-registry").await
            .expect("KV stream should exist after re-announce");
        let info2 = stream2.info().await.expect("stream info 2 failed");
        let got_ttl2 = info2.config.max_age;
        assert_eq!(
            got_ttl2, AGENTS_KV_TTL,
            "max_age after recreate should be {}s but got {}s ({}ns)",
            AGENTS_KV_TTL.as_secs(), got_ttl2.as_secs(), got_ttl2.as_nanos()
        );
    }

    // start_nats_js already runs with JetStream; basic NATS also works for request/reply
    async fn start_nats_plain() -> (NatsServer, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let child = Command::new("nats-server")
            .args(["-p", &port.to_string()])
            .spawn().expect("nats-server not in PATH");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        (NatsServer(child), format!("nats://127.0.0.1:{port}"))
    }

    #[tokio::test]
    async fn test_request_permission_approved() {
        let (_c, url) = start_nats_plain().await;
        let bridge = make_bridge(&url).await;

        // Spawn a mock Animus that approves all requests
        let responder_client = async_nats::connect(&url).await.unwrap();
        let mut sub = responder_client.subscribe(PERMISSION_REQUEST_SUBJECT).await.unwrap();
        tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(msg) = sub.next().await {
                if let Some(reply) = msg.reply {
                    let approval = r#"{"approved":true,"reason":"test approved"}"#;
                    let _ = responder_client.publish(reply, approval.into()).await;
                }
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let result = RequestPermission.execute(serde_json::json!({
            "action": "shell_exec",
            "details": "ls /tmp",
            "timeout_ms": 2000
        }), &bridge).await;

        assert!(!result.is_error, "expected approval, got: {}", result.content);
        let v: Value = serde_json::from_str(&result.content).unwrap();
        assert_eq!(v["approved"], true);
        assert_eq!(v["reason"], "test approved");
    }

    #[tokio::test]
    async fn test_request_permission_denied() {
        let (_c, url) = start_nats_plain().await;
        let bridge = make_bridge(&url).await;

        let responder_client = async_nats::connect(&url).await.unwrap();
        let mut sub = responder_client.subscribe(PERMISSION_REQUEST_SUBJECT).await.unwrap();
        tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(msg) = sub.next().await {
                if let Some(reply) = msg.reply {
                    let denial = r#"{"approved":false,"reason":"not safe"}"#;
                    let _ = responder_client.publish(reply, denial.into()).await;
                }
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let result = RequestPermission.execute(serde_json::json!({
            "action": "file_delete",
            "details": "rm -rf /important",
            "timeout_ms": 2000
        }), &bridge).await;

        assert!(result.is_error, "expected denial to be an error, got: {}", result.content);
        assert!(result.content.contains("not safe"), "unexpected denial message: {}", result.content);
    }

    #[tokio::test]
    async fn test_request_permission_timeout() {
        let (_c, url) = start_nats_plain().await;
        let bridge = make_bridge(&url).await;
        // No responder — should time out

        let result = RequestPermission.execute(serde_json::json!({
            "action": "shell_exec",
            "details": "test",
            "timeout_ms": 300
        }), &bridge).await;

        assert!(result.is_error);
        assert!(result.content.contains("timed out") || result.content.contains("No responder"),
            "unexpected message: {}", result.content);
    }

    #[tokio::test]
    async fn test_request_permission_missing_fields() {
        let (_c, url) = start_nats_plain().await;
        let bridge = make_bridge(&url).await;

        let r = RequestPermission.execute(serde_json::json!({ "action": "x" }), &bridge).await;
        assert!(r.is_error, "expected error for missing details");

        let r2 = RequestPermission.execute(serde_json::json!({ "details": "x" }), &bridge).await;
        assert!(r2.is_error, "expected error for missing action");
    }
}
