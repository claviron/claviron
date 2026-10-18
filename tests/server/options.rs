use super::{EnvironmentChoice, ServerOptions};

#[test]
fn registration_callbacks_append_in_order_and_stop_on_failure() {
    let calls = std::cell::RefCell::new(Vec::new());
    let registrations = super::chain_registration(
        super::chain_registration(
            |_: &()| {
                calls.borrow_mut().push("first");
                Ok(())
            },
            |_: &()| {
                calls.borrow_mut().push("second");
                Err(super::registry::Error::msg("registration rejected"))
            },
        ),
        |_: &()| {
            calls.borrow_mut().push("third");
            Ok(())
        },
    );
    assert!(registrations(&()).is_err());
    assert_eq!(*calls.borrow(), ["first", "second"]);
}

#[test]
fn typed_services_and_callbacks_compose_without_initializing_a_server() {
    struct Services;
    impl claviron_runtime::Register<claviron_runtime::AppState> for Services {
        fn register(
            _: &claviron_runtime::AppState
        ) -> super::registry::Result<()> {
            Ok(())
        }
    }
    let _server = ServerOptions::new()
        .build()
        .with_services::<(Services,)>()
        .register(|_| Ok(()))
        .with_services::<()>();
}

#[test]
fn production_is_only_chosen_explicitly() {
    use claviron_runtime::env::PRODUCTION;
    let options = ServerOptions::new();
    assert!(matches!(options.environment, EnvironmentChoice::Local));
    assert_eq!(
        options.resolved_environment().unwrap(),
        claviron_runtime::env::LOCAL
    );
    assert_eq!(
        ServerOptions::new().production().resolved_environment().unwrap(),
        PRODUCTION
    );
    for name in [PRODUCTION, "Production", "PRODUCTION"] {
        assert!(
            ServerOptions::new().environment(name).validate_primary().is_err(),
            "{name} cannot be named"
        );
    }
}

#[test]
fn deployment_selection_is_validated_before_startup() {
    for environment in ["staging", "preview", "test-2", "custom_environment"] {
        assert!(
            ServerOptions::new()
                .environment(environment)
                .validate_primary()
                .is_ok()
        );
    }
    for environment in
        ["", "../production", "two environments", "production/local"]
    {
        assert!(
            ServerOptions::new()
                .environment(environment)
                .validate_primary()
                .is_err()
        );
    }
}
