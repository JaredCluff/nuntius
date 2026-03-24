/// All configuration read from environment variables at startup.
#[derive(Debug, Clone)]
pub struct Config {
    /// NATS server URL. Default: nats://localhost:4222
    pub nats_url: String,
    /// Token authentication. Optional.
    pub auth_token: Option<String>,
    /// User for user/pass auth. Optional.
    pub user: Option<String>,
    /// Password for user/pass auth. Optional.
    pub pass: Option<String>,
    /// NKey seed. Optional.
    pub nkey: Option<String>,
    /// Path to TLS client certificate. Optional.
    pub tls_cert: Option<String>,
    /// Path to TLS client key. Optional.
    pub tls_key: Option<String>,
    /// Subjects to subscribe to on startup (comma-separated in env).
    pub startup_subs: Vec<String>,
    /// Default timeout for nats_request in milliseconds.
    pub request_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            nats_url: std::env::var("NUNTIUS_NATS_URL")
                .unwrap_or_else(|_| "nats://localhost:4222".to_string()),
            auth_token: std::env::var("NUNTIUS_AUTH_TOKEN").ok(),
            user: std::env::var("NUNTIUS_USER").ok(),
            pass: std::env::var("NUNTIUS_PASS").ok(),
            nkey: std::env::var("NUNTIUS_NKEY").ok(),
            tls_cert: std::env::var("NUNTIUS_TLS_CERT").ok(),
            tls_key: std::env::var("NUNTIUS_TLS_KEY").ok(),
            startup_subs: std::env::var("NUNTIUS_STARTUP_SUBS")
                .map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
                .unwrap_or_default(),
            request_timeout_ms: std::env::var("NUNTIUS_REQUEST_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        std::env::remove_var("NUNTIUS_NATS_URL");
        std::env::remove_var("NUNTIUS_REQUEST_TIMEOUT_MS");
        std::env::remove_var("NUNTIUS_STARTUP_SUBS");
        std::env::remove_var("NUNTIUS_AUTH_TOKEN");

        let cfg = Config::from_env();
        assert_eq!(cfg.nats_url, "nats://localhost:4222");
        assert_eq!(cfg.request_timeout_ms, 5000);
        assert!(cfg.startup_subs.is_empty());
        assert!(cfg.auth_token.is_none());
    }

    #[test]
    fn test_env_override() {
        std::env::set_var("NUNTIUS_NATS_URL", "nats://custom:4222");
        std::env::set_var("NUNTIUS_REQUEST_TIMEOUT_MS", "3000");
        std::env::set_var("NUNTIUS_STARTUP_SUBS", "foo.>,bar.*");

        let cfg = Config::from_env();
        assert_eq!(cfg.nats_url, "nats://custom:4222");
        assert_eq!(cfg.request_timeout_ms, 3000);
        assert_eq!(cfg.startup_subs, vec!["foo.>", "bar.*"]);

        std::env::remove_var("NUNTIUS_NATS_URL");
        std::env::remove_var("NUNTIUS_REQUEST_TIMEOUT_MS");
        std::env::remove_var("NUNTIUS_STARTUP_SUBS");
    }

    #[test]
    fn test_auth_token() {
        std::env::set_var("NUNTIUS_AUTH_TOKEN", "secret");
        let cfg = Config::from_env();
        assert_eq!(cfg.auth_token, Some("secret".to_string()));
        std::env::remove_var("NUNTIUS_AUTH_TOKEN");
    }
}
