use claviron_http::ext::PathPrefix;

use super::*;

/// A scope that redirects serves nothing: no owner and no file fallback are
/// prepared behind it, whatever the site declares.
#[test]
fn a_redirecting_scope_prepares_no_owner_and_no_fallback() {
    let state = AppState::standalone("redirect-plan", ".");
    let site: Site = serde_yaml::from_str(
        "redirect: {to: /new, code: 307}\nindex: [index.html]\n",
    )
    .unwrap();
    let root = PathPrefix::root();
    let plan = BackendPlan::prepare(
        &RouteContext {
            state: &state,
            host: None,
            config: &site,
            route: &site,
            prefix: &root,
        },
        &Default::default(),
    );
    assert!(plan.steps.is_empty());
    assert!(plan.fallback.is_none());
}

#[cfg(feature = "orbit")]
#[test]
fn an_orbit_route_without_the_application_link_fails_to_compile() {
    let state = AppState::standalone("orbit-unreachable", ".");
    let site: Site =
        serde_yaml::from_str("orbit: {fleet_name: first, application: root}\n")
            .unwrap();
    let error = compile_site(
        &state,
        Arc::new(site),
        SiteTerms {
            listener: None,
            policy: Some(HttpPolicy::unlimited()),
            transport: &TransportPolicy::default(),
        },
        None,
    )
    .err()
    .expect("nothing can reach this route");
    assert!(error.to_string().contains("application link"), "{error}");
}

/// A session plan reaches its owner alone: the host's site does not apply,
/// and its admission applies only where the profile asks for it.
#[tokio::test]
async fn a_session_plan_drops_the_site_and_inherits_only_what_it_names() {
    struct Owner;

    impl HostHandler for Owner {
        fn call(
            &self,
            _: crate::HostRequest,
        ) -> crate::HostHandlerFuture {
            Box::pin(std::future::ready(claviron_http::ServiceResult::Handled(
                claviron_http::err_status(http::StatusCode::NO_CONTENT),
            )))
        }
    }

    let state = AppState::standalone("session-plan", ".");
    // A site that redirects everything and admits no one.
    let site: Arc<Site> = Arc::new(
        serde_yaml::from_str(
            "redirect: {to: /new, code: 307}\nacl: {policy: deny}\n",
        )
        .unwrap(),
    );
    let host: Host = "example.test".parse().unwrap();
    let status = |inherit| {
        let execution = compile_session(SessionInput {
            state: &state,
            host: &host,
            site: site.clone(),
            transport: &TransportPolicy::default(),
            listener: None,
            http: None,
            inherit,
            layers: &[],
            owner: Arc::new(Owner),
            policy: HttpPolicy::unlimited(),
        })
        .unwrap();
        async move {
            // A client the host's ACL can judge, from a reserved address.
            let connection = Arc::new(claviron_http::ext::ConnCtx {
                transport: claviron_http::listener::ListenerTransport::Tcp,
                state: Default::default(),
                listener: "public".parse().unwrap(),
                proto: Some(claviron_http::listener::HttpProtocol::H2),
                remote: "192.0.2.1:1234".parse().unwrap(),
                local_ip: "127.0.0.1".parse().unwrap(),
                local_port: 8443,
                real_ip: None,
                country_code: None,
                tls: None,
            });
            let request = http::Request::builder()
                .uri("/anything")
                .extension(connection)
                .body(claviron_http::empty_body())
                .unwrap();
            execution.handle(request).await.unwrap().status()
        }
    };

    assert_eq!(status(Inherit::NONE).await, http::StatusCode::NO_CONTENT);
    assert_eq!(status(Inherit::ACL).await, http::StatusCode::FORBIDDEN);
}
