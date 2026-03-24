use crate::client::NatsBridge;
use super::{Tool, ToolResult};
use serde_json::Value;
use std::time::Duration;

fn require_str(params: &Value, key: &str) -> Result<String, ToolResult> {
    params[key].as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| ToolResult::err(format!("missing required field: {key}")))
}

// ── nats_publish ──────────────────────────────────────────────────────────────

pub struct NatsPublish;

#[async_trait::async_trait]
impl Tool for NatsPublish {
    fn name(&self) -> &str { "nats_publish" }
    fn description(&self) -> &str { "Fire-and-forget publish to a NATS subject." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["subject", "payload"],
            "properties": {
                "subject": { "type": "string" },
                "payload": { "type": "string" },
                "reply_to": { "type": "string" },
                "headers": { "type": "object" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let subject = match require_str(&params, "subject") { Ok(s) => s, Err(e) => return e };
        let payload = match require_str(&params, "payload") { Ok(s) => s, Err(e) => return e };

        let result = if let Some(reply_to) = params["reply_to"].as_str().map(|s| s.to_string()) {
            bridge.client()
                .publish_with_reply(subject, reply_to, payload.as_bytes().to_vec().into())
                .await
        } else {
            bridge.client()
                .publish(subject, payload.as_bytes().to_vec().into())
                .await
        };

        match result {
            Ok(_) => ToolResult::ok(serde_json::json!({ "ok": true })),
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── nats_request ─────────────────────────────────────────────────────────────

pub struct NatsRequest;

#[async_trait::async_trait]
impl Tool for NatsRequest {
    fn name(&self) -> &str { "nats_request" }
    fn description(&self) -> &str { "Publish to a subject and wait for a single reply." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["subject", "payload"],
            "properties": {
                "subject": { "type": "string" },
                "payload": { "type": "string" },
                "timeout_ms": { "type": "integer" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let subject = match require_str(&params, "subject") { Ok(s) => s, Err(e) => return e };
        let payload = match require_str(&params, "payload") { Ok(s) => s, Err(e) => return e };
        let timeout_ms = params["timeout_ms"].as_u64().unwrap_or(5000);

        let request_future = bridge.client()
            .request(subject, payload.as_bytes().to_vec().into());

        match tokio::time::timeout(Duration::from_millis(timeout_ms), request_future).await {
            Ok(Ok(response)) => {
                let body = std::str::from_utf8(&response.payload)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| {
                        use base64::Engine as _;
                        base64::engine::general_purpose::STANDARD.encode(&response.payload)
                    });
                ToolResult::ok(serde_json::json!({
                    "subject": response.subject.as_str(),
                    "payload": body,
                }))
            }
            Ok(Err(e)) => ToolResult::err(format!("no responders: {e}")),
            Err(_) => ToolResult::err(format!("timeout after {timeout_ms}ms")),
        }
    }
}

// ── nats_subscribe ────────────────────────────────────────────────────────────

pub struct NatsSubscribe;

#[async_trait::async_trait]
impl Tool for NatsSubscribe {
    fn name(&self) -> &str { "nats_subscribe" }
    fn description(&self) -> &str {
        "Subscribe to a NATS subject. Incoming messages appear as <channel> notifications in conversation context."
    }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["subject"],
            "properties": {
                "subject": { "type": "string", "description": "Supports wildcards: foo.*, foo.>" },
                "queue_group": { "type": "string" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let subject = match require_str(&params, "subject") { Ok(s) => s, Err(e) => return e };
        let queue_group = params["queue_group"].as_str();

        match bridge.subscribe(&subject, queue_group).await {
            Ok(sub_id) => ToolResult::ok(serde_json::json!({
                "subscription_id": sub_id,
                "subject": subject,
            })),
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── nats_unsubscribe ──────────────────────────────────────────────────────────

pub struct NatsUnsubscribe;

#[async_trait::async_trait]
impl Tool for NatsUnsubscribe {
    fn name(&self) -> &str { "nats_unsubscribe" }
    fn description(&self) -> &str { "Remove an active subscription by ID." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["subscription_id"],
            "properties": {
                "subscription_id": { "type": "string" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let sub_id = match require_str(&params, "subscription_id") { Ok(s) => s, Err(e) => return e };
        match bridge.unsubscribe(&sub_id) {
            Ok(_) => ToolResult::ok(serde_json::json!({ "ok": true })),
            Err(e) => ToolResult::err(e.to_string()),
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

    async fn start_nats() -> (NatsServer, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let child = Command::new("nats-server")
            .args(["-p", &port.to_string()])
            .spawn().expect("nats-server not in PATH");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        (NatsServer(child), format!("nats://127.0.0.1:{port}"))
    }

    async fn make_bridge(url: &str) -> (NatsBridge, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let cfg = Config { nats_url: url.to_string(), ..Config::from_env() };
        let (tx, rx) = mpsc::unbounded_channel();
        let bridge = NatsBridge::connect(&cfg, tx).await.unwrap();
        (bridge, rx)
    }

    #[tokio::test]
    async fn test_nats_publish_ok() {
        let (_c, url) = start_nats().await;
        let (bridge, _rx) = make_bridge(&url).await;
        let result = NatsPublish.execute(
            serde_json::json!({ "subject": "test.pub", "payload": "hello" }),
            &bridge,
        ).await;
        assert!(!result.is_error, "publish failed: {}", result.content);
        assert!(result.content.contains("true"));
    }

    #[tokio::test]
    async fn test_nats_request_reply() {
        let (_c, url) = start_nats().await;
        let (bridge, _rx) = make_bridge(&url).await;

        // Subscribe first (await so the sub is registered before we send the request)
        let client = async_nats::connect(&url).await.unwrap();
        let mut sub = client.subscribe("test.rpc").await.unwrap();
        // Yield once to let NATS propagate the subscription server-side
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Spawn the responder loop after subscription is established
        tokio::spawn(async move {
            use futures::StreamExt;
            if let Some(msg) = sub.next().await {
                if let Some(reply) = msg.reply {
                    client.publish(reply, "pong".into()).await.unwrap();
                }
            }
        });

        let result = NatsRequest.execute(
            serde_json::json!({ "subject": "test.rpc", "payload": "ping", "timeout_ms": 2000 }),
            &bridge,
        ).await;
        assert!(!result.is_error, "request failed: {}", result.content);
        assert!(result.content.contains("pong"));
    }

    #[tokio::test]
    async fn test_nats_request_timeout() {
        let (_c, url) = start_nats().await;
        let (bridge, _rx) = make_bridge(&url).await;
        let result = NatsRequest.execute(
            serde_json::json!({ "subject": "test.nobody", "payload": "ping", "timeout_ms": 300 }),
            &bridge,
        ).await;
        assert!(result.is_error, "expected error on timeout, got: {}", result.content);
    }

    #[tokio::test]
    async fn test_nats_subscribe_and_unsubscribe() {
        let (_c, url) = start_nats().await;
        let (bridge, _rx) = make_bridge(&url).await;

        let sub_result = NatsSubscribe.execute(
            serde_json::json!({ "subject": "test.sub" }),
            &bridge,
        ).await;
        assert!(!sub_result.is_error, "subscribe failed: {}", sub_result.content);
        let v: serde_json::Value = serde_json::from_str(&sub_result.content).unwrap();
        let sub_id = v["subscription_id"].as_str().unwrap().to_string();

        let unsub_result = NatsUnsubscribe.execute(
            serde_json::json!({ "subscription_id": sub_id }),
            &bridge,
        ).await;
        assert!(!unsub_result.is_error, "unsubscribe failed: {}", unsub_result.content);

        // Second unsubscribe should fail (already removed)
        let unsub2 = NatsUnsubscribe.execute(
            serde_json::json!({ "subscription_id": sub_id }),
            &bridge,
        ).await;
        assert!(unsub2.is_error, "expected error on double unsubscribe");
    }
}
