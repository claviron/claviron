use super::*;

#[test]
fn normalization_preserves_the_documented_segment_contract() {
    for (raw, expected) in
        [("/", "/"), ("///", "/"), ("/api/", "/api"), ("/api///", "/api")]
    {
        let prefix =
            PathPrefix::deserialize(serde::de::value::StrDeserializer::<
                serde::de::value::Error,
            >::new(raw))
            .unwrap();
        assert_eq!(prefix.as_str(), expected);
        assert!(prefix.has_path_segment(expected));
        assert!(prefix.has_path_segment(&format!("{expected}/child")));
        if expected != "/" {
            assert!(!prefix.has_path_segment("/apix"));
        }
    }
}
