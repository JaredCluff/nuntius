use crate::{client::NatsBridge, tools::{Tool, ToolResult}};
use serde_json::Value;

pub struct KvPut;
pub struct KvGet;
pub struct KvDelete;
pub struct KvKeys;

macro_rules! stub_tool {
    ($name:ident, $tool_name:literal) => {
        #[async_trait::async_trait]
        impl Tool for $name {
            fn name(&self) -> &str { $tool_name }
            fn description(&self) -> &str { "Not yet implemented" }
            fn input_schema(&self) -> Value { serde_json::json!({"type":"object","properties":{}}) }
            async fn execute(&self, _params: Value, _bridge: &NatsBridge) -> ToolResult {
                ToolResult::err("not implemented")
            }
        }
    };
}

stub_tool!(KvPut, "kv_put");
stub_tool!(KvGet, "kv_get");
stub_tool!(KvDelete, "kv_delete");
stub_tool!(KvKeys, "kv_keys");
