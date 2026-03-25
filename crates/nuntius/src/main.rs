use nuntius_core::{Config, NatsBridge, tools::ToolRegistry};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

fn to_json_line(v: &serde_json::Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| {
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal serialization error"}}"#.to_string()
    })
}

fn send_or_log(tx: &mpsc::UnboundedSender<String>, msg: String) {
    if tx.send(msg).is_err() {
        tracing::warn!("stdout writer has exited; dropping response");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr) // MUST write to stderr; stdout is the MCP channel
        .init();

    let config = Config::from_env();

    // Single channel → single stdout writer task (prevents interleaving)
    let (stdout_tx, mut stdout_rx) = mpsc::unbounded_channel::<String>();

    let stdout_writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = stdout_rx.recv().await {
            if stdout.write_all(line.as_bytes()).await.is_err()
                || stdout.write_all(b"\n").await.is_err()
                || stdout.flush().await.is_err()
            {
                tracing::warn!("stdout write failed, closing writer");
                break;
            }
        }
    });

    let bridge = match NatsBridge::connect(&config, stdout_tx.clone()).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("NATS connection failed: {e}");
            std::process::exit(1);
        }
    };

    for subject in &config.startup_subs {
        if let Err(e) = bridge.subscribe(subject, None).await {
            eprintln!("Startup subscription failed for {subject}: {e}");
        }
    }

    let registry = ToolRegistry::new();
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim().to_string();
        if line.is_empty() { continue; }

        let request: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                let err = error_response(None, -32700, &format!("Parse error: {e}"));
                send_or_log(&stdout_tx, to_json_line(&err));
                continue;
            }
        };

        let id = request.get("id").cloned();
        let method = request["method"].as_str().unwrap_or("");

        let response = match method {
            "initialize" => handle_initialize(id, &request),
            "notifications/initialized" => continue,
            "ping" => handle_ping(id),
            "tools/list" => handle_tools_list(id, &registry),
            "tools/call" => handle_tools_call(id, &request, &bridge, &registry).await,
            _ => error_response(id.as_ref(), -32601, "Method not found"),
        };

        send_or_log(&stdout_tx, to_json_line(&response));
    }

    drop(stdout_tx);
    let _ = stdout_writer.await;
    Ok(())
}

fn handle_initialize(id: Option<serde_json::Value>, request: &serde_json::Value) -> serde_json::Value {
    // Echo back the client's requested protocol version so Claude Code uses the
    // version it expects, enabling all features including notifications/claude/channel.
    const FALLBACK_VERSION: &str = "2024-11-05";
    const SUPPORTED: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05", "2024-10-07"];
    let requested = request["params"]["protocolVersion"].as_str().unwrap_or(FALLBACK_VERSION);
    let protocol_version = if SUPPORTED.contains(&requested) { requested } else { FALLBACK_VERSION };

    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": protocol_version,
            "capabilities": { "tools": {}, "experimental": { "claude/channel": {} } },
            "serverInfo": { "name": "nuntius", "version": env!("CARGO_PKG_VERSION") }
        }
    })
}

fn handle_ping(id: Option<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": {} })
}

fn handle_tools_list(id: Option<serde_json::Value>, registry: &ToolRegistry) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": { "tools": registry.list_definitions() }
    })
}

async fn handle_tools_call(
    id: Option<serde_json::Value>,
    request: &serde_json::Value,
    bridge: &NatsBridge,
    registry: &ToolRegistry,
) -> serde_json::Value {
    let name = request["params"]["name"].as_str().unwrap_or("");
    let arguments = request["params"]["arguments"].clone();

    match registry.get(name) {
        Some(tool) => {
            let result = tool.execute(arguments, bridge).await;
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [{ "type": "text", "text": result.content }],
                    "isError": result.is_error
                }
            })
        }
        None => error_response(id.as_ref(), -32601, &format!("Unknown tool: {name}")),
    }
}

fn error_response(
    id: Option<&serde_json::Value>,
    code: i32,
    message: &str,
) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    })
}
