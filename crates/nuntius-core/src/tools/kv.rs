use crate::client::NatsBridge;
use super::{Tool, ToolResult};
use async_nats::jetstream;
use serde_json::Value;

pub struct KvPut;
pub struct KvGet;
pub struct KvDelete;
pub struct KvKeys;

async fn get_or_create_bucket(
    bridge: &NatsBridge,
    bucket: &str,
) -> Result<jetstream::kv::Store, String> {
    let js = async_nats::jetstream::new(bridge.client().clone());
    // Try to get existing bucket first; create it if it doesn't exist yet.
    match js.get_key_value(bucket).await {
        Ok(store) => Ok(store),
        Err(_) => js.create_key_value(jetstream::kv::Config {
            bucket: bucket.to_string(),
            ..Default::default()
        }).await.map_err(|e| e.to_string()),
    }
}

// ── kv_put ────────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Tool for KvPut {
    fn name(&self) -> &str { "kv_put" }
    fn description(&self) -> &str { "Store a value in a JetStream KV bucket." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["bucket", "key", "value"],
            "properties": {
                "bucket": { "type": "string" },
                "key": { "type": "string" },
                "value": { "type": "string" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let bucket = match params["bucket"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing bucket"),
        };
        let key = match params["key"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing key"),
        };
        let value = match params["value"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing value"),
        };

        let kv = match get_or_create_bucket(bridge, bucket).await {
            Ok(kv) => kv,
            Err(e) => return ToolResult::err(e),
        };

        match kv.put(key, value.as_bytes().to_vec().into()).await {
            Ok(revision) => ToolResult::ok(serde_json::json!({ "revision": revision })),
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── kv_get ────────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Tool for KvGet {
    fn name(&self) -> &str { "kv_get" }
    fn description(&self) -> &str { "Retrieve a value from a JetStream KV bucket." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["bucket", "key"],
            "properties": {
                "bucket": { "type": "string" },
                "key": { "type": "string" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let bucket = match params["bucket"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing bucket"),
        };
        let key = match params["key"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing key"),
        };

        let kv = match get_or_create_bucket(bridge, bucket).await {
            Ok(kv) => kv,
            Err(e) => return ToolResult::err(e),
        };

        // After delete, kv.entry() returns Some(entry) with Operation::Delete — not None.
        // Must check operation to distinguish live vs deleted keys.
        match kv.entry(key).await {
            Ok(Some(entry)) if entry.operation == jetstream::kv::Operation::Put => {
                let value = std::str::from_utf8(&entry.value)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| {
                        use base64::Engine as _;
                        base64::engine::general_purpose::STANDARD.encode(&entry.value)
                    });
                ToolResult::ok(serde_json::json!({
                    "key": key,
                    "value": value,
                    "revision": entry.revision,
                }))
            }
            Ok(_) => ToolResult::err(format!("key not found: {key}")),
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── kv_delete ─────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Tool for KvDelete {
    fn name(&self) -> &str { "kv_delete" }
    fn description(&self) -> &str { "Delete a key from a JetStream KV bucket." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["bucket", "key"],
            "properties": {
                "bucket": { "type": "string" },
                "key": { "type": "string" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let bucket = match params["bucket"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing bucket"),
        };
        let key = match params["key"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing key"),
        };

        let kv = match get_or_create_bucket(bridge, bucket).await {
            Ok(kv) => kv,
            Err(e) => return ToolResult::err(e),
        };

        match kv.delete(key).await {
            Ok(_) => ToolResult::ok(serde_json::json!({ "ok": true })),
            Err(e) => ToolResult::err(e.to_string()),
        }
    }
}

// ── kv_keys ───────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Tool for KvKeys {
    fn name(&self) -> &str { "kv_keys" }
    fn description(&self) -> &str { "List keys in a JetStream KV bucket, with optional prefix filter." }
    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["bucket"],
            "properties": {
                "bucket": { "type": "string" },
                "prefix": { "type": "string", "description": "Optional key prefix filter" }
            }
        })
    }

    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult {
        let bucket = match params["bucket"].as_str() {
            Some(s) => s,
            None => return ToolResult::err("missing bucket"),
        };
        let prefix = params["prefix"].as_str();

        let kv = match get_or_create_bucket(bridge, bucket).await {
            Ok(kv) => kv,
            Err(e) => return ToolResult::err(e),
        };

        match kv.keys().await {
            Ok(mut keys_stream) => {
                use futures::StreamExt;
                let mut keys = Vec::new();
                while let Some(key) = keys_stream.next().await {
                    match key {
                        Ok(k) => {
                            if prefix.map(|p| k.starts_with(p)).unwrap_or(true) {
                                keys.push(k);
                            }
                        }
                        Err(e) => return ToolResult::err(e.to_string()),
                    }
                }
                ToolResult::ok(serde_json::json!({ "keys": keys }))
            }
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
    async fn test_kv_lifecycle() {
        let (_c, url) = start_nats_js().await;
        let bridge = make_bridge(&url).await;

        // put
        let put = KvPut.execute(serde_json::json!({
            "bucket": "test-bucket", "key": "mykey", "value": "myvalue"
        }), &bridge).await;
        assert!(!put.is_error, "put failed: {}", put.content);
        let put_v: serde_json::Value = serde_json::from_str(&put.content).unwrap();
        assert!(put_v["revision"].is_number());

        // get
        let get = KvGet.execute(serde_json::json!({
            "bucket": "test-bucket", "key": "mykey"
        }), &bridge).await;
        assert!(!get.is_error, "get failed: {}", get.content);
        assert!(get.content.contains("myvalue"));

        // keys
        let keys = KvKeys.execute(serde_json::json!({ "bucket": "test-bucket" }), &bridge).await;
        assert!(!keys.is_error, "keys failed: {}", keys.content);
        assert!(keys.content.contains("mykey"));

        // delete
        let del = KvDelete.execute(serde_json::json!({
            "bucket": "test-bucket", "key": "mykey"
        }), &bridge).await;
        assert!(!del.is_error, "delete failed: {}", del.content);

        // get after delete should error
        let get2 = KvGet.execute(serde_json::json!({
            "bucket": "test-bucket", "key": "mykey"
        }), &bridge).await;
        assert!(get2.is_error, "expected error after delete, got: {}", get2.content);
    }
}
