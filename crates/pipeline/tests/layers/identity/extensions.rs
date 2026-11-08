use super::*;
use crate::Site;
use claviron_http::ext::{
    ConnCtx, HostKey, HostName, RealIpExt, RequestArrival,
};
use claviron_http::policy::HttpPolicy;
use http::{Extensions, Request};
use std::sync::Arc;

struct VhostCtx {
    hostname: HostName,
    vhost_key: HostKey,
}

fn direct() -> (Arc<ConnCtx>, VhostCtx) {
    let host = VhostCtx {
        hostname: HostName(Arc::new("alias.test".parse().unwrap())),
        vhost_key: HostKey(Arc::new("app.test".parse().unwrap())),
    };
    let connection = Arc::new(ConnCtx {
        transport: claviron_http::listener::ListenerTransport::Tcp,
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(claviron_http::listener::HttpProtocol::H1),
        remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 8080,
        real_ip: None,
        country_code: Some("GB".into()),
        tls: None,
    });
    (connection, host)
}

fn export(
    request: &mut Request<()>,
    connection: &Arc<ConnCtx>,
    host: &VhostCtx,
) {
    let (mut head, body) = std::mem::take(request).into_parts();
    inject(
        &mut head.extensions,
        HttpPolicy::unlimited(),
        Some(
            HostContext::new(connection, RequestArrival::Established)
                .with_host(&host.hostname, &host.vhost_key),
        ),
    );
    *request = Request::from_parts(head, body);
}

fn assert_context(
    extensions: &Extensions,
    connection: &Arc<ConnCtx>,
    host: &VhostCtx,
) {
    assert!(Arc::ptr_eq(extensions.get::<Arc<ConnCtx>>().unwrap(), connection));
    assert!(Arc::ptr_eq(
        &extensions.get::<HostName>().unwrap().0,
        &host.hostname.0
    ));
    // The canonical key selects the host inside the edge; no backend reads it.
    assert!(extensions.get::<HostKey>().is_none());
    assert_eq!(extensions.get::<HttpPolicy>(), Some(&HttpPolicy::unlimited()));
    assert!(extensions.get::<Arc<Site>>().is_none());
    assert!(extensions.get::<claviron_http::ListenerId>().is_none());
    assert!(
        extensions.get::<claviron_http::listener::HttpProtocol>().is_none()
    );
    assert!(extensions.get::<http::uri::Scheme>().is_none());
}

#[test]
fn identity_exports_shared_runtime_owners_without_retaining_host_configuration()
{
    let (connection, host) = direct();
    let mut first = Request::new(());
    let mut second = Request::new(());
    export(&mut first, &connection, &host);
    export(&mut second, &connection, &host);
    assert_eq!(first.extensions().len(), 3);
    assert_context(first.extensions(), &connection, &host);
    assert_context(second.extensions(), &connection, &host);
    assert!(first.extensions().get::<RealIpExt>().is_none());
    assert_eq!(
        claviron_http::ext::request_real_ip(first.extensions()),
        Some(connection.remote.ip())
    );
    first.extensions_mut().clear();
    assert_context(second.extensions(), &connection, &host);
}

#[derive(Clone)]
struct NativeValue(Arc<()>);

#[test]
fn identity_preserves_forwarded_ip_and_reuses_existing_connection_extension_box()
 {
    let (connection, host) = direct();
    let native = Arc::new(());
    let previous = Arc::new(connection.as_ref().clone());
    let previous_weak = Arc::downgrade(&previous);
    let forwarded = "203.0.113.1".parse().unwrap();
    let mut request = Request::builder()
        .extension(NativeValue(native.clone()))
        .extension(previous)
        .extension(RealIpExt(forwarded))
        .body(())
        .unwrap();
    let connection_address = request.extensions().get::<Arc<ConnCtx>>().unwrap()
        as *const Arc<ConnCtx>;
    let ip_address =
        request.extensions().get::<RealIpExt>().unwrap() as *const RealIpExt;
    export(&mut request, &connection, &host);
    assert_context(request.extensions(), &connection, &host);
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
        claviron_http::ext::request_real_ip(request.extensions()),
        Some(forwarded)
    );
    assert!(previous_weak.upgrade().is_none());
    assert!(Arc::ptr_eq(
        &request.extensions().get::<NativeValue>().unwrap().0,
        &native
    ));
    assert_eq!(connection.country_code.as_deref(), Some("GB"));
    assert!(connection.real_ip.is_none());
}

#[test]
fn identity_without_transport_context_keeps_explicit_caller_metadata() {
    let mut request = Request::builder()
        .extension(RealIpExt("203.0.113.1".parse().unwrap()))
        .body(())
        .unwrap();
    let (mut head, _) = std::mem::take(&mut request).into_parts();
    inject(&mut head.extensions, HttpPolicy::unlimited(), None);
    assert_eq!(head.extensions.len(), 2);
    assert!(head.extensions.get::<Arc<ConnCtx>>().is_none());
    assert_eq!(
        claviron_http::ext::request_real_ip(&head.extensions),
        Some("203.0.113.1".parse().unwrap())
    );
}
