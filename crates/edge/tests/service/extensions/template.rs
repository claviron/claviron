use super::*;
use claviron_http::config::HttpPolicy;
use claviron_http::ext::ReceivedAt;

fn direct() -> (Arc<ConnCtx>, VhostCtx) {
    let table =
        serde_yaml::from_str("app.test: {root: public, aliases: [alias.test]}")
            .unwrap();
    let host = resolve_sni_from_table(&table, Some("alias.test")).unwrap();
    let connection = Arc::new(ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(claviron_tls::Proto::H1),
        remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 8080,
        real_ip: Some("192.0.2.1".parse().unwrap()),
        country_code: None,
        tls: None,
    });
    (connection, host)
}

fn assert_context(
    extensions: &Extensions,
    connection: &Arc<ConnCtx>,
    host: &VhostCtx,
) {
    assert!(Arc::ptr_eq(extensions.get::<Arc<ConnCtx>>().unwrap(), connection));
    assert_eq!(
        extensions.get::<claviron_http::ListenerId>(),
        Some(&connection.listener)
    );
    assert_eq!(
        extensions.get::<claviron_tls::Proto>(),
        connection.proto.as_ref()
    );
    assert_eq!(
        extensions.get::<http::uri::Scheme>(),
        Some(&connection.scheme())
    );
    assert_eq!(
        extensions.get::<RealIpExt>().unwrap().0,
        connection.real_ip.unwrap()
    );
    assert!(Arc::ptr_eq(
        &extensions.get::<HostName>().unwrap().0,
        &host.hostname.0
    ));
    assert!(Arc::ptr_eq(
        &extensions.get::<HostKey>().unwrap().0,
        &host.vhost_key.0
    ));
    assert!(Arc::ptr_eq(
        extensions.get::<Arc<HostConfig>>().unwrap(),
        &host.vhost_config
    ));
}

#[test]
fn prepared_extension_maps_keep_all_types_and_independent_request_ownership() {
    let (connection, host) = direct();
    let template = identity_pipeline(&connection, &host, None);
    let mut first = Request::new(());
    let mut second = Request::new(());
    inject(&template, &mut first);
    inject(&template, &mut second);
    assert_eq!(first.extensions().len(), 8);
    assert_eq!(second.extensions().len(), 8);
    assert_context(first.extensions(), &connection, &host);
    assert_context(second.extensions(), &connection, &host);
    assert!(!std::ptr::eq(
        first.extensions().get::<HostName>().unwrap(),
        second.extensions().get::<HostName>().unwrap()
    ));
    first.extensions_mut().remove::<HostName>();
    first.extensions_mut().clear();
    assert_context(second.extensions(), &connection, &host);
    let mut third = Request::new(());
    inject(&template, &mut third);
    assert_context(third.extensions(), &connection, &host);
    assert!(third.extensions().get::<HttpPolicy>().is_none());
    assert!(third.extensions().get::<ReceivedAt>().is_none());
}

#[derive(Clone)]
struct NativeValue(Arc<()>);

#[test]
fn existing_extensions_keep_their_boxes_and_unrelated_values() {
    let (connection, host) = direct();
    let template = identity_pipeline(&connection, &host, None);
    let native = Arc::new(());
    let policy = HttpPolicy::unlimited();
    let received = ReceivedAt(std::time::SystemTime::UNIX_EPOCH);
    let previous_connection = Arc::new(connection.as_ref().clone());
    let previous_connection_weak = Arc::downgrade(&previous_connection);
    let mut request = Request::builder()
        .extension(NativeValue(native.clone()))
        .extension(policy)
        .extension(received)
        .extension(previous_connection)
        .extension(RealIpExt("203.0.113.1".parse().unwrap()))
        .extension(http::uri::Scheme::HTTPS)
        .body(())
        .unwrap();
    let address = request.extensions().get::<NativeValue>().unwrap()
        as *const NativeValue;
    let connection_address = request.extensions().get::<Arc<ConnCtx>>().unwrap()
        as *const Arc<ConnCtx>;
    let ip_address =
        request.extensions().get::<RealIpExt>().unwrap() as *const RealIpExt;
    let scheme_address =
        request.extensions().get::<http::uri::Scheme>().unwrap()
            as *const http::uri::Scheme;
    inject(&template, &mut request);
    assert_context(request.extensions(), &connection, &host);
    let value = request.extensions().get::<NativeValue>().unwrap();
    assert_eq!(value as *const NativeValue, address);
    assert_eq!(
        request.extensions().get::<Arc<ConnCtx>>().unwrap()
            as *const Arc<ConnCtx>,
        connection_address
    );
    assert_eq!(
        request.extensions().get::<RealIpExt>().unwrap() as *const RealIpExt,
        ip_address
    );
    assert_eq!(
        request.extensions().get::<http::uri::Scheme>().unwrap()
            as *const http::uri::Scheme,
        scheme_address
    );
    assert!(previous_connection_weak.upgrade().is_none());
    assert!(Arc::ptr_eq(&value.0, &native));
    assert_eq!(request.extensions().get::<HttpPolicy>(), Some(&policy));
    assert_eq!(request.extensions().get::<ReceivedAt>(), Some(&received));
}

#[test]
fn empty_allocated_extensions_receive_the_complete_context() {
    let (connection, host) = direct();
    let template = identity_pipeline(&connection, &host, None);
    let mut request = Request::builder()
        .extension(NativeValue(Arc::new(())))
        .body(())
        .unwrap();
    request.extensions_mut().clear();
    inject(&template, &mut request);
    assert_eq!(request.extensions().len(), 8);
    assert_context(request.extensions(), &connection, &host);
}
