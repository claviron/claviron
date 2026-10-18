use super::*;

#[test]
fn the_default_policy_offers_both_versions_and_the_full_suite_list() {
    let policy = TlsPolicy::default();

    assert_eq!(policy.min_version, TlsVersion::Tls12);
    assert_eq!(policy.max_version, TlsVersion::Tls13);
    assert_eq!(versions().len(), 2);
    assert_eq!(
        provider().cipher_suites.len(),
        aws_lc_rs::default_provider().cipher_suites.len()
    );
}

#[test]
fn an_inverted_version_range_is_rejected_at_boot() {
    let policy = TlsPolicy {
        min_version: TlsVersion::Tls13,
        max_version: TlsVersion::Tls12,
        ..TlsPolicy::default()
    };

    assert!(policy.validate().is_err());
}

#[test]
fn a_misspelled_suite_is_rejected_rather_than_silently_dropped() {
    let policy = TlsPolicy {
        cipher_suites: vec![
            "TLS13_AES_256_GCM_SHA384".to_owned(),
            "TLS13_TYPO".to_owned(),
        ],
        ..TlsPolicy::default()
    };

    let error = policy.validate().unwrap_err().to_string();
    assert!(error.contains("TLS13_TYPO"), "{error}");
}

#[test]
fn names_are_the_iana_spellings_rustls_reports() {
    let policy = TlsPolicy {
        cipher_suites: vec!["TLS13_AES_256_GCM_SHA384".to_owned()],
        ..TlsPolicy::default()
    };

    assert!(policy.validate().is_ok());
}
