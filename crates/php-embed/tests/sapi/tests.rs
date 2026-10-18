use super::callbacks::sanitize_log_message;

#[test]
fn application_log_drops_html_document_case_insensitively() {
    let message = "RuntimeException: broken\n<HTML><body>rendered error</body></HTML>\ntrace";

    assert_eq!(
        sanitize_log_message(message),
        "RuntimeException: broken\n\ntrace"
    );
}

#[test]
fn application_log_bounds_multibyte_messages() {
    let message = "ö".repeat(16 * 1024);
    let sanitized = sanitize_log_message(&message);

    assert!(sanitized.ends_with(" [truncated]"));
    assert!(sanitized.is_char_boundary(sanitized.len()));
}
