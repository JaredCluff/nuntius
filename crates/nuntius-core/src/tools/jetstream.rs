use crate::client::NatsBridge;
use super::{Tool, ToolResult};
use async_nats::jetstream;
use futures::StreamExt;
use serde_json::Value;
use std::time::Duration;

fn get_js(bridge: &NatsBridge) -> jetstream::Context {
    async_nats::jetstream::new(bridge.client().clone())
}

// ── js_publish ────────────────────────────────────────────────────────────────

pub struct JsPublish;

#[async_trait::async_trait]
impl Tool for JsPublish {
    fn name(&self) -> &str { "js_publish" }
    fn description(&self) -> &str { "Publish with persistence guarantee to a JetStream stream." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["subject", "payload"],
            "properties": {
                "subject": { "type": "string" },
                "payload": { "type": "string" },
                "msg_id": { "type": "string", "description": "Deduplication ID" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let subject = match params["subject"].as_str().map(|s| s.to_string()) {
            Some(s) => s,
            None => return ToolResult::err("missing subject"),
        };
        let payload_vec: Vec<u8> = match params["payload"].as_str() {
            Some(s) => s.as_bytes().to_vec(),
            None => return ToolResult::err("missing payload"),
        };

        let js = get_js(bridge);

        let ack_future = if let Some(msg_id) = params["msg_id"].as_str() {
            let publish = jetstream::context::Publish::build()
                .payload(payload_vec.into())
                .message_id(msg_id);
            match js.send_publish(subject, publish).await {
                Ok(f) => f,
                Err(e) => return ToolResult::err(e.to_string()),
            }
        } else {
            match js.publish(subject, payload_vec.into()).await {
                Ok(f) => f,
                Err(e) => return ToolResult::err(e.to_string()),
            }
        };

        match ack_future.await {
            Ok(ack) => ToolResult::ok(serde_json::json!({
                "stream": ack.stream,
                "seq": ack.sequence,
                "duplicate": ack.duplicate,
            })),
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── js_stream_create ──────────────────────────────────────────────────────────

pub struct JsStreamCreate;

#[async_trait::async_trait]
impl Tool for JsStreamCreate {
    fn name(&self) -> &str { "js_stream_create" }
    fn description(&self) -> &str { "Create a JetStream stream." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["name", "subjects"],
            "properties": {
                "name": { "type": "string" },
                "subjects": { "type": "array", "items": { "type": "string" } },
                "max_msgs": { "type": "integer" },
                "max_bytes": { "type": "integer" },
                "max_age_secs": { "type": "integer" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let name = match params["name"].as_str().map(|s| s.to_string()) {
            Some(s) => s,
            None => return ToolResult::err("missing name"),
        };
        let subjects: Vec<String> = match params["subjects"].as_array() {
            Some(arr) => arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect(),
            None => return ToolResult::err("missing subjects array"),
        };

        let mut config = jetstream::stream::Config {
            name: name.clone(),
            subjects,
            ..Default::default()
        };
        if let Some(v) = params["max_msgs"].as_i64() { config.max_messages = v; }
        if let Some(v) = params["max_bytes"].as_i64() { config.max_bytes = v; }
        if let Some(v) = params["max_age_secs"].as_u64() {
            config.max_age = Duration::from_secs(v);
        }

        match get_js(bridge).create_stream(config).await {
            Ok(stream) => {
                let info = stream.cached_info();
                ToolResult::ok(serde_json::json!({
                    "name": info.config.name,
                    "subjects": info.config.subjects,
                }))
            },
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── js_stream_info ────────────────────────────────────────────────────────────

pub struct JsStreamInfo;

#[async_trait::async_trait]
impl Tool for JsStreamInfo {
    fn name(&self) -> &str { "js_stream_info" }
    fn description(&self) -> &str { "Get info and stats for a JetStream stream." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["name"],
            "properties": { "name": { "type": "string" } }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let name = match params["name"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing name"),
        };
        let js = get_js(bridge);
        match js.get_stream(name).await {
            Ok(mut stream) => {
                match stream.info().await {
                    Ok(info) => ToolResult::ok(serde_json::json!({
                        "name": info.config.name,
                        "subjects": info.config.subjects,
                        "messages": info.state.messages,
                        "bytes": info.state.bytes,
                    })),
                    Err(e) => ToolResult::err(e.to_string()),
                }
            },
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── js_stream_delete ──────────────────────────────────────────────────────────

pub struct JsStreamDelete;

#[async_trait::async_trait]
impl Tool for JsStreamDelete {
    fn name(&self) -> &str { "js_stream_delete" }
    fn description(&self) -> &str { "Delete a JetStream stream and all its messages." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["name"],
            "properties": { "name": { "type": "string" } }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let name = match params["name"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing name"),
        };
        match get_js(bridge).delete_stream(name).await {
            Ok(_) => ToolResult::ok(serde_json::json!({ "ok": true })),
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── js_consume ────────────────────────────────────────────────────────────────

pub struct JsConsume;

#[async_trait::async_trait]
impl Tool for JsConsume {
    fn name(&self) -> &str { "js_consume" }
    fn description(&self) -> &str { "Pull up to N messages from a JetStream stream." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["stream"],
            "properties": {
                "stream": { "type": "string" },
                "consumer_name": { "type": "string", "description": "Durable consumer name; ephemeral if omitted" },
                "batch": { "type": "integer", "default": 1 },
                "timeout_ms": { "type": "integer", "default": 5000 }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let stream_name = match params["stream"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing stream"),
        };
        let batch = params["batch"].as_u64().unwrap_or(1) as usize;
        let timeout_ms = params["timeout_ms"].as_u64().unwrap_or(5000);
        let consumer_name = params["consumer_name"].as_str().map(|s| s.to_string());

        let js = get_js(bridge);
        let stream = match js.get_stream(stream_name).await {
            Ok(s) => s,
            Err(e) => return ToolResult::err(e.to_string()),
        };

        let consumer_config = jetstream::consumer::pull::Config {
            durable_name: consumer_name,
            ..Default::default()
        };
        let consumer = match stream.create_consumer(consumer_config).await {
            Ok(c) => c,
            Err(e) => return ToolResult::err(e.to_string()),
        };

        let fetch_result = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            async {
                let mut messages = consumer
                    .fetch()
                    .max_messages(batch)
                    .messages()
                    .await?;
                let mut results = Vec::new();
                while let Some(Ok(msg)) = messages.next().await {
                    let payload = std::str::from_utf8(&msg.payload)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| {
                            use base64::Engine as _;
                            base64::engine::general_purpose::STANDARD.encode(&msg.payload)
                        });
                    let seq = msg.info().ok().map(|i| i.stream_sequence).unwrap_or(0);
                    let headers = match &msg.headers {
                        Some(hmap) => {
                            let mut m = serde_json::Map::new();
                            for (k, vs) in hmap.iter() {
                                let vals: Vec<Value> = vs.iter()
                                    .map(|v| Value::String(v.to_string()))
                                    .collect();
                                m.insert(k.to_string(), Value::Array(vals));
                            }
                            Value::Object(m)
                        }
                        None => Value::Object(serde_json::Map::new()),
                    };
                    results.push(serde_json::json!({
                        "subject": msg.subject.as_str(),
                        "payload": payload,
                        "seq": seq,
                        "headers": headers,
                    }));
                    let _ = msg.ack().await;
                    if results.len() >= batch { break; }
                }
                Ok::<_, async_nats::Error>(results)
            }
        ).await;

        match fetch_result {
            Ok(Ok(msgs)) => ToolResult::ok(Value::Array(msgs)),
            Ok(Err(e)) => ToolResult::err(e.to_string()),
            Err(_) => ToolResult::ok(Value::Array(vec![])), // timeout = empty result
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
        let (tx, _rx) = mpsc::unbounded_channel();
        NatsBridge::connect(&cfg, tx).await.unwrap()
    }

    #[tokio::test]
    async fn test_stream_lifecycle() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        // Create stream
        let create = JsStreamCreate.execute(serde_json::json!({
            "name": "TEST",
            "subjects": ["test.js.>"]
        }), &bridge).await;
        assert!(!create.is_error, "create failed: {}", create.content);

        // Info
        let info = JsStreamInfo.execute(serde_json::json!({ "name": "TEST" }), &bridge).await;
        assert!(!info.is_error, "info failed: {}", info.content);
        assert!(info.content.contains("TEST"));

        // Delete
        let del = JsStreamDelete.execute(serde_json::json!({ "name": "TEST" }), &bridge).await;
        assert!(!del.is_error, "delete failed: {}", del.content);
    }

    #[tokio::test]
    async fn test_js_publish_and_consume() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        // Use a unique stream name and msg_id to avoid JetStream duplicate detection
        // across test runs that share the same temp storage directory.
        let stream_name = format!("WORK{}", uuid::Uuid::new_v4().as_simple());
        let msg_id = uuid::Uuid::new_v4().to_string();
        let subject = format!("{}.task", stream_name.to_lowercase());

        // Create stream first
        JsStreamCreate.execute(serde_json::json!({
            "name": stream_name,
            "subjects": [format!("{}.>", stream_name.to_lowercase())]
        }), &bridge).await;

        // Publish
        let pub_result = JsPublish.execute(serde_json::json!({
            "subject": subject,
            "payload": "do something",
            "msg_id": msg_id
        }), &bridge).await;
        assert!(!pub_result.is_error, "publish failed: {}", pub_result.content);
        let v: serde_json::Value = serde_json::from_str(&pub_result.content).unwrap();
        assert_eq!(v["stream"], stream_name);
        assert_eq!(v["duplicate"], false);

        // Consume
        let consume = JsConsume.execute(serde_json::json!({
            "stream": stream_name,
            "batch": 1,
            "timeout_ms": 2000
        }), &bridge).await;
        assert!(!consume.is_error, "consume failed: {}", consume.content);
        let msgs: serde_json::Value = serde_json::from_str(&consume.content).unwrap();
        let arr = msgs.as_array().unwrap();
        assert_eq!(arr.len(), 1, "expected 1 message, got {}", arr.len());
        assert!(arr[0]["payload"].as_str().unwrap().contains("do something"));
        assert!(arr[0]["seq"].is_number(), "seq should be a number");
        assert!(arr[0]["headers"].is_object(), "headers should be an object");
    }
}
