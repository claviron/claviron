use super::*;
use claviron_http::config::{HttpSnapshot, ListenerSettings, TcpSecurity};
use futures_util::FutureExt;

#[test]
fn explicit_listener_requires_selection_without_weakening_host_filters() {
    let state = AppState::standalone("explicit-listener", ".");
    let http: HttpSnapshot =
        serde_yaml::from_str(super::protocol::LISTENERS).unwrap();
    let registry = HostAppRegistry::default();
    let table = serde_yaml::from_str(
        "auto.test: {aliases: [auto-alias.test], transport: {non_ssl: serve}}\npicked.test: {listeners: [plain, tls, quic], aliases: [picked-alias.test], transport: {non_ssl: serve}}\nother.test: {listeners: plain-alt}\ndisabled.test: {listeners: []}\n",
    ).unwrap();
    for name in ["plain", "tls", "quic"] {
        let id = name.parse().unwrap();
        let mut input =
            crate::transport::publication::select_listener(&http, &id).unwrap();
        assert!(!input.listeners[&id].explicit);
        let automatic =
            registry.prepare_listener(&state, &table, &input, None).unwrap();
        Arc::make_mut(input.listeners.get_mut(&id).unwrap()).explicit = true;
        let explicit = registry
            .prepare_listener(&state, &table, &input, Some(&automatic))
            .unwrap();
        for name in ["auto.test", "auto-alias.test"] {
            let host = name.parse().unwrap();
            assert!(
                automatic.resolve_application_on(&host, Some(&id)).is_some()
            );
            assert!(
                explicit.resolve_application_on(&host, Some(&id)).is_none()
            );
        }
        for name in ["picked.test", "picked-alias.test"] {
            let host = name.parse().unwrap();
            assert!(
                automatic.resolve_application_on(&host, Some(&id)).is_some()
            );
            assert!(
                explicit.resolve_application_on(&host, Some(&id)).is_some()
            );
        }
        for name in ["other.test", "disabled.test"] {
            let host = name.parse().unwrap();
            assert!(
                automatic.resolve_application_on(&host, Some(&id)).is_none()
            );
            assert!(
                explicit.resolve_application_on(&host, Some(&id)).is_none()
            );
        }
    }
}

#[test]
#[cfg(feature = "tcp")]
fn plain_listener_policies_skip_application_factories_and_keep_bodies_unread() {
    use crate::loader::selection::resolve_sni_for_listener;
    use claviron_http::ext::ConnCtx;
    use claviron_http::ext::RequestArrival;
    use claviron_http::listener::HttpProtocol;
    use claviron_pipeline::HostContext;
    use http_body::{Body, Frame};
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct Unread(Arc<AtomicUsize>);
    impl Body for Unread {
        type Data = bytes::Bytes;
        type Error = Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>>
        {
            panic!("terminal plain policies must not read the body")
        }
    }
    impl Drop for Unread {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let state = AppState::standalone("plain-policies", ".");
    let http: HttpSnapshot =
        serde_yaml::from_str(super::protocol::LISTENERS).unwrap();
    let id = "plain".parse().unwrap();
    let mut input =
        crate::transport::publication::select_listener(&http, &id).unwrap();
    let table = serde_yaml::from_str(
        "redirect.test: {aliases: [alias.test], transport: {protocols: h3}}\nreject.test: {transport: {non_ssl: reject, protocols: h3}}\nserve.test: {transport: {non_ssl: serve}}\n'2001:db8::1': {}\nexcluded.test: {listeners: tls}\n",
    ).unwrap();
    let registry = HostAppRegistry::default();
    for host in ["redirect.test", "reject.test", "2001:db8::1"] {
        registry.register_other(host, |_, _, _| {
            Err(Error::msg("terminal HTTP policy must not prepare the router"))
        });
    }
    registry.register_router("serve.test", |_| {
        Router::new().fallback(|| async { StatusCode::CREATED })
    });
    let prepared =
        registry.prepare_listener(&state, &table, &input, None).unwrap();
    assert!(
        prepared
            .resolve_application_on(
                &"excluded.test".parse().unwrap(),
                Some(&id)
            )
            .is_none()
    );
    let connection = Arc::new(ConnCtx {
        state: Default::default(),
        listener: id.clone(),
        proto: Some(HttpProtocol::H1),
        remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "192.0.2.2".parse().unwrap(),
        local_port: 8080,
        real_ip: None,
        country_code: None,
        tls: None,
    });
    let drops = Arc::new(AtomicUsize::new(0));
    let policy = input.listeners[&id].policy();
    let pipeline =
        crate::host::NonServingPipeline::new(443.try_into().unwrap(), None);
    let respond = |hosts: &crate::transport::VhostTable,
                   pipeline: &crate::host::NonServingPipeline,
                   name: &str| {
        let mut selected =
            resolve_sni_for_listener(hosts, Some(name), &id).unwrap();
        let request = Request::builder()
            .uri("/a%2Fb?x=1")
            .body(claviron_http::boxed_body(Unread(drops.clone())))
            .unwrap();
        if let Some(app) = selected.application.take() {
            let context =
                HostContext::new(&connection, RequestArrival::Established)
                    .with_host(&selected.hostname, &selected.vhost_key);
            app.handle_in(request, Some(context))
                .now_or_never()
                .unwrap()
                .unwrap()
        } else {
            pipeline
                .call(request, selected, connection.clone(), policy)
                .now_or_never()
                .unwrap()
        }
    };
    let redirected = respond(&prepared, &pipeline, "alias.test");
    assert_eq!(redirected.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(
        redirected.headers()["location"],
        "https://alias.test/a%2Fb?x=1"
    );
    assert_eq!(
        respond(&prepared, &pipeline, "reject.test").status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        respond(&prepared, &pipeline, "serve.test").status(),
        StatusCode::CREATED
    );
    let ipv6 = respond(&prepared, &pipeline, "2001:db8::1");
    assert_eq!(ipv6.headers()["location"], "https://[2001:db8::1]/a%2Fb?x=1");
    assert_eq!(drops.load(Ordering::Relaxed), 4);

    let ListenerSettings::Tcp(tcp) =
        &mut Arc::make_mut(input.listeners.get_mut(&id).unwrap()).settings
    else {
        unreachable!()
    };
    tcp.security =
        TcpSecurity::Cleartext { redirect_port: 8443.try_into().unwrap() };
    let next = registry
        .prepare_listener(&state, &table, &input, Some(&prepared))
        .unwrap();
    let next_pipeline =
        crate::host::NonServingPipeline::new(8443.try_into().unwrap(), None);
    assert_eq!(
        respond(&next, &next_pipeline, "alias.test").headers()["location"],
        "https://alias.test:8443/a%2Fb?x=1"
    );
    assert_eq!(
        respond(&prepared, &pipeline, "alias.test").headers()["location"],
        "https://alias.test/a%2Fb?x=1"
    );
    struct Interception(Arc<AtomicUsize>);
    impl crate::InterceptsNonServing for Interception {
        fn intercept<'a>(
            &'a self,
            request: claviron_pipeline::HostRequest,
            context: crate::NonServingContext<'a>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = claviron_pipeline::HostHandlerResult,
                    > + Send
                    + 'a,
            >,
        > {
            assert_eq!(context.key.0.to_string(), "reject.test");
            assert_eq!(
                context.config.transport.non_ssl,
                crate::config::NonSslPolicy::Reject
            );
            assert!(request.head().extensions.is_empty());
            self.0.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                if request.head().uri.path() == "/pending" {
                    std::future::pending::<()>().await;
                }
                if request.head().uri.path() == "/challenge" {
                    drop(request);
                    ServiceResult::Handled(claviron_http::err_status(
                        StatusCode::NO_CONTENT,
                    ))
                } else {
                    ServiceResult::Unhandled(request)
                }
            })
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let interception = Arc::new(Interception(calls.clone()));
    bind_interception(&registry, interception.clone()).unwrap();
    assert!(bind_interception(&registry, interception).is_err());
    let intercepted =
        registry.prepare_listener(&state, &table, &input, None).unwrap();
    let binding = intercepted
        .prepared::<HostAppSnapshot>()
        .unwrap()
        .non_serving_interception();
    let pipeline =
        crate::host::NonServingPipeline::new(8443.try_into().unwrap(), binding);
    assert_eq!(
        respond(&intercepted, &pipeline, "reject.test").status(),
        StatusCode::FORBIDDEN
    );
    for path in ["/challenge", "/pending"] {
        let selected =
            resolve_sni_for_listener(&intercepted, Some("reject.test"), &id)
                .unwrap();
        let before = drops.load(Ordering::Relaxed);
        let response = pipeline
            .call(
                Request::builder()
                    .uri(path)
                    .body(Unread(drops.clone()))
                    .unwrap(),
                selected,
                connection.clone(),
                policy,
            )
            .now_or_never();
        if path == "/challenge" {
            assert_eq!(response.unwrap().status(), StatusCode::NO_CONTENT);
        } else {
            assert!(response.is_none());
        }
        assert_eq!(drops.load(Ordering::Relaxed), before + 1);
    }
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[test]
#[cfg(feature = "tcp")]
fn non_serving_factory_is_cold_optional_and_retains_published_generations() {
    use crate::{InterceptsNonServing, NonServingContext};
    use claviron_pipeline::{HostHandlerResult, HostRequest};
    struct Interception;
    impl InterceptsNonServing for Interception {
        fn intercept<'a>(
            &'a self,
            request: HostRequest,
            _: NonServingContext<'a>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = HostHandlerResult> + Send + 'a,
            >,
        > {
            Box::pin(std::future::ready(
                claviron_http::ServiceResult::Unhandled(request),
            ))
        }
    }
    let state = AppState::standalone("non-serving-factory", ".");
    let http: HttpSnapshot =
        serde_yaml::from_str(super::protocol::LISTENERS).unwrap();
    let table = serde_yaml::from_str("example.test: {root: public}").unwrap();
    let registry = HostAppRegistry::default();
    let mode = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let interception: Arc<dyn InterceptsNonServing> = Arc::new(Interception);
    registry
        .bind_non_serving_interception_with({
            let (mode, calls, interception) =
                (mode.clone(), calls.clone(), interception.clone());
            move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                match mode.load(Ordering::Relaxed) {
                    0 => Ok(None),
                    1 => Ok(Some(interception.clone())),
                    _ => Err(Error::msg("invalid interception configuration")),
                }
            }
        })
        .unwrap();
    assert!(bind_interception(&registry, interception.clone()).is_err());
    let input = crate::transport::publication::select_listener(
        &http,
        &"plain".parse().unwrap(),
    )
    .unwrap();
    let inactive =
        registry.prepare_listener(&state, &table, &input, None).unwrap();
    assert!(
        inactive
            .prepared::<HostAppSnapshot>()
            .unwrap()
            .non_serving_interception()
            .is_none()
    );
    mode.store(1, Ordering::Relaxed);
    let active = registry
        .prepare_listener(&state, &table, &input, Some(&inactive))
        .unwrap();
    let retained = active.prepared::<HostAppSnapshot>().unwrap();
    assert!(Arc::ptr_eq(
        &retained.non_serving_interception().unwrap(),
        &interception
    ));
    mode.store(0, Ordering::Relaxed);
    let disabled = registry
        .prepare_listener(&state, &table, &input, Some(&active))
        .unwrap();
    assert!(
        disabled
            .prepared::<HostAppSnapshot>()
            .unwrap()
            .non_serving_interception()
            .is_none()
    );
    assert!(Arc::ptr_eq(
        &retained.non_serving_interception().unwrap(),
        &interception
    ));
    mode.store(2, Ordering::Relaxed);
    assert!(
        registry
            .prepare_listener(&state, &table, &input, Some(&active))
            .is_err()
    );
    assert!(retained.non_serving_interception().is_some());
    let before = calls.load(Ordering::Relaxed);
    for name in ["tls", "quic"] {
        let input = crate::transport::publication::select_listener(
            &http,
            &name.parse().unwrap(),
        )
        .unwrap();
        let selected =
            registry.prepare_listener(&state, &table, &input, None).unwrap();
        assert!(
            selected
                .prepared::<HostAppSnapshot>()
                .unwrap()
                .non_serving_interception()
                .is_none()
        );
    }
    assert_eq!(calls.load(Ordering::Relaxed), before);
}

fn bind_interception(
    registry: &HostAppRegistry,
    interception: Arc<dyn crate::InterceptsNonServing>,
) -> claviron_runtime::registry::Result<()> {
    registry.bind_non_serving_interception_with(move |_| {
        Ok(Some(interception.clone()))
    })
}
