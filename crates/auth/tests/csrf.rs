use super::Csrf;

#[test]
fn regression_csrf_scope_is_framed_and_nonce_length_is_fixed() {
    let csrf = Csrf::new([7; 32]);
    let token = csrf.issue(Some(b"scope-one"));
    assert!(csrf.verify(&token, Some(b"scope-one")));
    assert!(!csrf.verify(&token, Some(b"scope-two")));
    let rest = token.strip_prefix("v1.").unwrap();
    let (nonce, mac) = rest.split_once('.').unwrap();
    let rebound = format!("v1.{nonce}{}.{mac}", hex::encode(b"scope-one"));
    assert!(!csrf.verify(&rebound, None));
    assert!(
        !csrf.verify(
            &format!("v1.{}.{}", &nonce[..30], mac),
            Some(b"scope-one")
        )
    );
    assert!(
        !csrf.verify(&format!("v1.{nonce}.{}", &mac[..62]), Some(b"scope-one"))
    );
    assert!(
        !csrf.verify(
            &format!("v1.{nonce}.{}", "g".repeat(64)),
            Some(b"scope-one")
        )
    );
    assert!(
        !csrf.verify(rest, Some(b"scope-one")),
        "unversioned tokens are retired"
    );
    let guest = csrf.issue(None);
    assert!(csrf.verify(&guest, None));
    assert!(!csrf.verify(&guest, Some(b"")));
    let empty_scope = csrf.issue(Some(b""));
    assert!(csrf.verify(&empty_scope, Some(b"")));
    assert!(!csrf.verify(&empty_scope, None));
    assert!(!Csrf::new([8; 32]).verify(&token, Some(b"scope-one")));
}
