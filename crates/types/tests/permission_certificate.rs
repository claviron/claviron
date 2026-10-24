use super::*;
use crate::permission::PermissionCatalog;

#[test]
fn signed_scope_codec_preserves_absence_and_explicit_empty() {
    assert_eq!(
        from_uris(["urn:uuid:00000000-0000-4000-8000-000000000001"]).unwrap(),
        None
    );
    let mut catalog = PermissionCatalog::new();
    catalog.register_name("service.read").unwrap();
    let selected = catalog.validate(["service.read"]).unwrap();
    assert_eq!(
        from_uris([to_uri(&selected).as_str()]).unwrap(),
        Some(selected)
    );
    let empty = catalog.validate(Vec::<String>::new()).unwrap();
    assert_eq!(from_uris([to_uri(&empty).as_str()]).unwrap(), Some(empty));
    assert!(from_uris(["urn:claviron:permissions:v2:service.read"]).is_err());
    assert!(from_uris(["urn:claviron:permissions:v1:service.*"]).is_err());
    assert!(
        from_uris([
            "urn:claviron:permissions:v1:",
            "urn:claviron:permissions:v1:"
        ])
        .is_err()
    );
}
