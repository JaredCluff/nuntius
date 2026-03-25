use crate::Config;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tracing::debug;

struct SubscriptionHandle {
    abort: AbortHandle,
}

struct BridgeState {
    subscriptions: HashMap<String, SubscriptionHandle>,
}

/// Wraps an async-nats client, holding active subscriptions and a channel
/// to forward inbound NATS messages as MCP notifications.
#[derive(Clone)]
pub struct NatsBridge {
    client: async_nats::Client,
    state: Arc<Mutex<BridgeState>>,
    notification_tx: mpsc::UnboundedSender<String>,
}

impl NatsBridge {
    /// Connect to NATS using config. notification_tx receives formatted
    /// MCP notification JSON strings whenever a subscribed message arrives.
    pub async fn connect(
        config: &Config,
        notification_tx: mpsc::UnboundedSender<String>,
    ) -> anyhow::Result<Self> {
        let mut options = async_nats::ConnectOptions::new();

        if let Some(token) = &config.auth_token {
            options = options.token(token.clone());
        } else if let (Some(user), Some(pass)) = (&config.user, &config.pass) {
            options = options.user_and_password(user.clone(), pass.clone());
        } else if let Some(nkey_seed) = &config.nkey {
            // async-nats 0.38: nkey() takes the seed String directly
            options = options.nkey(nkey_seed.clone());
        }

        let client = options.connect(&config.nats_url).await?;
        debug!("Connected to NATS at {}", config.nats_url);

        Ok(Self {
            client,
            state: Arc::new(Mutex::new(BridgeState {
                subscriptions: HashMap::new(),
            })),
            notification_tx,
        })
    }

    /// Expose the raw async-nats client for tool use.
    pub fn client(&self) -> &async_nats::Client {
        &self.client
    }

    /// Add a live subscription. Returns a stable subscription ID (UUID).
    /// Messages arriving on `subject` will be pushed as MCP channel notifications.
    pub async fn subscribe(
        &self,
        subject: &str,
        queue_group: Option<&str>,
    ) -> anyhow::Result<String> {
        let sub_id = uuid::Uuid::new_v4().to_string();
        let subject_owned = subject.to_string();

        let mut subscriber = match queue_group {
            Some(group) => self.client.queue_subscribe(subject_owned, group.to_string()).await?,
            None => self.client.subscribe(subject_owned).await?,
        };

        let notification_tx = self.notification_tx.clone();
        let sub_id_for_task = sub_id.clone();

        let handle = tokio::spawn(async move {
            while let Some(msg) = subscriber.next().await {
                let body = std::str::from_utf8(&msg.payload)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| BASE64.encode(&msg.payload));
                let ts = Utc::now().to_rfc3339();
                let mut meta = serde_json::json!({
                    "subject": msg.subject.as_str(),
                    "ts": ts,
                });
                if let Some(reply_to) = msg.reply.as_deref() {
                    meta["reply_to"] = serde_json::Value::String(reply_to.to_string());
                }
                let envelope = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/claude/channel",
                    "params": {
                        "content": body,
                        "meta": meta
                    }
                });
                let json = match serde_json::to_string(&envelope) {
                    Ok(j) => j,
                    Err(_) => continue,
                };
                if notification_tx.send(json).is_err() {
                    debug!("Notification channel closed, stopping subscription {}", sub_id_for_task);
                    break;
                }
            }
        });

        self.state.lock().subscriptions.insert(
            sub_id.clone(),
            SubscriptionHandle { abort: handle.abort_handle() },
        );

        Ok(sub_id)
    }

    /// Remove a subscription by ID. Returns error if ID not found.
    pub fn unsubscribe(&self, sub_id: &str) -> anyhow::Result<()> {
        let mut state = self.state.lock();
        match state.subscriptions.remove(sub_id) {
            Some(handle) => {
                handle.abort.abort();
                Ok(())
            }
            None => anyhow::bail!("subscription not found: {}", sub_id),
        }
    }

    /// Check if a subscription ID exists.
    pub fn has_subscription(&self, sub_id: &str) -> bool {
        self.state.lock().subscriptions.contains_key(sub_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command};
    use std::net::TcpListener;

    struct NatsServer(Child);

    impl Drop for NatsServer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    async fn start_nats() -> (NatsServer, String) {
        // Bind to port 0 to get a free port, then release it for nats-server
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let child = Command::new("nats-server")
            .args(["-p", &port.to_string(), "-js"])
            .spawn()
            .expect("nats-server not found — install with: brew install nats-server");

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let url = format!("nats://127.0.0.1:{port}");
        (NatsServer(child), url)
    }

    #[tokio::test]
    async fn test_connect_and_ping() {
        let (_c, url) = start_nats().await;
        let cfg = Config { nats_url: url, ..Config::from_env() };
        let (tx, _rx) = mpsc::unbounded_channel();
        let bridge = NatsBridge::connect(&cfg, tx).await.unwrap();
        bridge.client().publish("test.ping", "ok".into()).await.unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_and_receive_notification() {
        let (_c, url) = start_nats().await;
        let cfg = Config { nats_url: url.clone(), ..Config::from_env() };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let bridge = NatsBridge::connect(&cfg, tx).await.unwrap();

        let sub_id = bridge.subscribe("test.notify", None).await.unwrap();
        assert!(!sub_id.is_empty());

        // Publish a message from a separate client
        let client = async_nats::connect(&url).await.unwrap();
        client.publish("test.notify", "hello".into()).await.unwrap();

        let notif = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rx.recv(),
        ).await.unwrap().unwrap();

        // The notification should be a notifications/claude/channel JSON envelope
        let envelope: serde_json::Value = serde_json::from_str(&notif).unwrap();
        assert_eq!(envelope["method"], "notifications/claude/channel");
        assert_eq!(envelope["params"]["content"], "hello");
        assert_eq!(envelope["params"]["meta"]["subject"], "test.notify");
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let (_c, url) = start_nats().await;
        let cfg = Config { nats_url: url.clone(), ..Config::from_env() };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let bridge = NatsBridge::connect(&cfg, tx).await.unwrap();

        let sub_id = bridge.subscribe("test.unsub", None).await.unwrap();
        bridge.unsubscribe(&sub_id).unwrap();

        // After unsubscribe, messages should NOT produce notifications
        let client = async_nats::connect(&url).await.unwrap();
        client.publish("test.unsub", "ignored".into()).await.unwrap();

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            rx.recv(),
        ).await;
        assert!(result.is_err(), "should have timed out — no notification after unsubscribe");
    }
}
