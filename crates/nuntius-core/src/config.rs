#[derive(Debug, Clone)]
pub struct Config {
    pub nats_url: String,
    pub auth_token: Option<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub nkey: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub startup_subs: Vec<String>,
    pub request_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            nats_url: "nats://localhost:4222".to_string(),
            auth_token: None,
            user: None,
            pass: None,
            nkey: None,
            tls_cert: None,
            tls_key: None,
            startup_subs: vec![],
            request_timeout_ms: 5000,
        }
    }
}
