use crate::client::NatsBridge;
use super::{Tool, ToolResult};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::Value;
use std::time::Duration;

pub struct AgentAnnounce;
pub struct AgentDiscover;
pub struct AgentClaim;

const AGENTS_KV_BUCKET: &str = "agents-registry";
const AGENTS_ANNOUNCE_SUBJECT: &str = "agents.registry.announce";
const CLAIM_QUEUE_GROUP: &str = "nuntius.claim";

// Same try-then-create pattern as kv.rs — get_or_create_key_value doesn't exist in async-nats 0.38
async fn get_or_create_agents_kv(bridge: &NatsBridge) -> Result<jetstream::kv::Store, String> {
    let js = async_nats::jetstream::new(bridge.client().clone());
    match js.get_key_value(AGENTS_KV_BUCKET).await {
        Ok(store) => return Ok(store),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            if !msg.contains("not found") && !msg.contains("stream not found") && !msg.contains("bucket not found") {
                return Err(e.to_string());
            }
        }
    }
    match js.create_key_value(jetstream::kv::Config {
        bucket: AGENTS_KV_BUCKET.to_string(),
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

        let record = serde_json::json!({
            "agent_id": agent_id,
            "capabilities": capabilities,
            "metadata": metadata,
            "last_seen": last_seen,
        });

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
        let child = Command::new("nats-server")
            .args(["-p", &port.to_string(), "-js"])
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

    #[tokio::test]
    async fn test_claim_missing_subject() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        let r = AgentClaim.execute(serde_json::json!({}), &bridge).await;
        assert!(r.is_error, "expected error for missing subject");
    }
}
