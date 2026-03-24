//! Integration tests: spawn the nuntius binary, speak MCP over stdin/stdout.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    (NatsServer(child), format!("nats://127.0.0.1:{port}"))
}

fn nuntius_bin() -> std::path::PathBuf {
    // Target dir is workspace root / target / debug / nuntius
    let manifest = env!("CARGO_MANIFEST_DIR");
    std::path::Path::new(manifest).join("../../target/debug/nuntius")
}

async fn spawn_nuntius(nats_url: &str) -> tokio::process::Child {
    // Build the binary first
    Command::new("cargo")
        .args(["build", "-p", "nuntius"])
        .current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .status().expect("cargo build failed");

    tokio::process::Command::new(nuntius_bin())
        .env("NUNTIUS_NATS_URL", nats_url)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn nuntius")
}

async fn send_recv(
    stdin: &mut tokio::process::ChildStdin,
    stdout: &mut BufReader<tokio::process::ChildStdout>,
    msg: serde_json::Value,
) -> serde_json::Value {
    let line = serde_json::to_string(&msg).unwrap() + "\n";
    stdin.write_all(line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
    let mut response_line = String::new();
    stdout.read_line(&mut response_line).await.unwrap();
    serde_json::from_str(response_line.trim()).unwrap()
}

#[tokio::test]
async fn test_mcp_initialize() {
    let (_nats, url) = start_nats().await;
    let mut child = spawn_nuntius(&url).await;
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    let response = send_recv(&mut stdin, &mut reader, serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "test", "version": "0.1" }
        }
    })).await;

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    assert_eq!(response["result"]["serverInfo"]["name"], "nuntius");

    child.kill().await.unwrap();
}

#[tokio::test]
async fn test_mcp_tools_list() {
    let (_nats, url) = start_nats().await;
    let mut child = spawn_nuntius(&url).await;
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    // Initialize first (required by MCP protocol)
    send_recv(&mut stdin, &mut reader, serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name":"t","version":"0"} }
    })).await;

    let response = send_recv(&mut stdin, &mut reader, serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}
    })).await;

    let tools = response["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 16, "Expected 16 tools, got {}", tools.len());
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"nats_publish"));
    assert!(names.contains(&"agent_claim"));

    child.kill().await.unwrap();
}

#[tokio::test]
async fn test_mcp_tool_call_publish() {
    let (_nats, url) = start_nats().await;
    let mut child = spawn_nuntius(&url).await;
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    send_recv(&mut stdin, &mut reader, serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name":"t","version":"0"} }
    })).await;

    let response = send_recv(&mut stdin, &mut reader, serde_json::json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "nats_publish",
            "arguments": { "subject": "test.integration", "payload": "hi" }
        }
    })).await;

    assert_eq!(response["id"], 3);
    let content = response["result"]["content"][0]["text"].as_str().unwrap();
    assert!(content.contains("true"), "Expected ok:true in: {content}");

    child.kill().await.unwrap();
}
