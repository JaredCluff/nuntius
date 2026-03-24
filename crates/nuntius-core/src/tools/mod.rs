pub mod core;
pub mod jetstream;
pub mod kv;
pub mod agent;

use crate::client::NatsBridge;
use serde_json::Value;

/// Result of a tool execution.
pub struct ToolResult {
    pub content: String,
    pub is_error: bool,
}

impl ToolResult {
    pub fn ok(v: Value) -> Self {
        Self { content: v.to_string(), is_error: false }
    }
    pub fn err(msg: impl Into<String>) -> Self {
        Self { content: msg.into(), is_error: true }
    }
}

/// A tool the MCP server can call.
#[async_trait::async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn input_schema(&self) -> Value;
    async fn execute(&self, params: Value, bridge: &NatsBridge) -> ToolResult;
}

/// Registry of all available tools.
pub struct ToolRegistry {
    tools: Vec<Box<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        let mut r = Self { tools: Vec::new() };
        r.register(Box::new(core::NatsPublish));
        r.register(Box::new(core::NatsRequest));
        r.register(Box::new(core::NatsSubscribe));
        r.register(Box::new(core::NatsUnsubscribe));
        r.register(Box::new(jetstream::JsPublish));
        r.register(Box::new(jetstream::JsStreamCreate));
        r.register(Box::new(jetstream::JsStreamInfo));
        r.register(Box::new(jetstream::JsStreamDelete));
        r.register(Box::new(jetstream::JsConsume));
        r.register(Box::new(kv::KvPut));
        r.register(Box::new(kv::KvGet));
        r.register(Box::new(kv::KvDelete));
        r.register(Box::new(kv::KvKeys));
        r.register(Box::new(agent::AgentAnnounce));
        r.register(Box::new(agent::AgentDiscover));
        r.register(Box::new(agent::AgentClaim));
        r
    }

    pub fn register(&mut self, tool: Box<dyn Tool>) {
        self.tools.push(tool);
    }

    pub fn get(&self, name: &str) -> Option<&dyn Tool> {
        self.tools.iter().find(|t| t.name() == name).map(|t| t.as_ref())
    }

    /// Returns tool definitions for the MCP tools/list response.
    pub fn list_definitions(&self) -> Vec<Value> {
        self.tools.iter().map(|t| serde_json::json!({
            "name": t.name(),
            "description": t.description(),
            "inputSchema": t.input_schema(),
        })).collect()
    }
}

impl Default for ToolRegistry {
    fn default() -> Self { Self::new() }
}
