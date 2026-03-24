use crate::{client::NatsBridge, tools::{Tool, ToolResult}};
use serde_json::Value;

pub struct JsPublish;
pub struct JsStreamCreate;
pub struct JsStreamInfo;
pub struct JsStreamDelete;
pub struct JsConsume;

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

stub_tool!(JsPublish, "js_publish");
stub_tool!(JsStreamCreate, "js_stream_create");
stub_tool!(JsStreamInfo, "js_stream_info");
stub_tool!(JsStreamDelete, "js_stream_delete");
stub_tool!(JsConsume, "js_consume");
