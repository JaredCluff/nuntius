use chrono::Utc;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

/// Format an incoming NATS message as a <channel> block for MCP notification delivery.
pub fn format_channel_notification(
    subject: &str,
    payload: &[u8],
    reply_to: Option<&str>,
) -> String {
    let ts = Utc::now().to_rfc3339();
    let body = std::str::from_utf8(payload)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| BASE64.encode(payload));

    let reply_attr = reply_to
        .map(|r| format!(" reply_to=\"{}\"", r))
        .unwrap_or_default();

    let escaped_body = body.replace('&', "&amp;").replace('<', "&lt;");
    format!(
        "<channel source=\"nats\" subject=\"{}\" ts=\"{}\"{}>{}</channel>",
        subject, ts, reply_attr, escaped_body
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_payload() {
        let result = format_channel_notification("foo.bar", b"hello world", None);
        assert!(result.contains("source=\"nats\""));
        assert!(result.contains("subject=\"foo.bar\""));
        assert!(result.contains("hello world"));
        assert!(!result.contains("reply_to"));
    }

    #[test]
    fn test_reply_to_included() {
        let result = format_channel_notification("foo", b"data", Some("_INBOX.abc"));
        assert!(result.contains("reply_to=\"_INBOX.abc\""));
    }

    #[test]
    fn test_binary_falls_back_to_base64() {
        let binary = vec![0xFF, 0xFE, 0x00];
        let result = format_channel_notification("foo", &binary, None);
        // base64 of [0xFF, 0xFE, 0x00] is "//4A"
        assert!(result.contains("//4A"));
    }

    #[test]
    fn test_body_xml_chars_escaped() {
        let payload = b"</channel><script>alert(1)</script>";
        let result = format_channel_notification("foo", payload, None);
        assert!(result.contains("&lt;/channel>"));
        assert!(!result.contains("</channel><script>"));
    }
}
