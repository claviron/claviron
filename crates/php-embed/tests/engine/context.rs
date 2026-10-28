use super::*;

#[test]
fn sapi_reads_the_pinned_execution_values_not_the_raw_route_declaration() {
    use claviron_runtime::congif::dotenw::Snapshot;
    let runtime = crate::engine::tests::runtime(std::path::PathBuf::from("."));
    let current = Some(RequestEnvironment(Snapshot::new(
        [("VALUE".into(), "original".into())].into(),
    )));
    let next = Some(RequestEnvironment(Snapshot::new(
        [("VALUE".into(), "replacement".into())].into(),
    )));
    assert_eq!(
        RequestEnvironment::values(&current, &runtime)
            .get("VALUE")
            .map(String::as_str),
        Some("original")
    );
    assert_eq!(
        RequestEnvironment::values(&next, &runtime)
            .get("VALUE")
            .map(String::as_str),
        Some("replacement")
    );
    assert_eq!(
        RequestEnvironment::values(&current, &runtime)
            .get("VALUE")
            .map(String::as_str),
        Some("original")
    );
    assert!(RequestEnvironment::values(&None, &runtime).is_empty());
}

fn program(
    model: ProgramModel,
    current: Option<PhpWorkUnit>,
) -> ClavironContext {
    ClavironContext {
        execution: PhpExecution::Program(PhpProgramContext {
            model,
            name: "p".to_owned(),
            instance: 0,
            script: std::path::PathBuf::from("p.php"),
            environment: std::collections::BTreeMap::new(),
            cancellation: CancellationToken::new(),
            current,
            #[cfg(feature = "tasks")]
            output: None,
        }),
        event_subscription: None,
        #[cfg(feature = "tasks")]
        invocation_subscription: Mutex::new(None),
        env_overlay: Mutex::new(HashMap::new()),
        request_finished: false,
    }
}

/// The state the old two-variant enum could not name: an application
/// serving a request and the same application idle between messages were
/// both `Program { .. }`, so nothing could tell them apart. Everything
/// here is a plain field read that no compiler would miss if it were lost.
#[test]
fn an_application_is_on_http_only_while_it_holds_a_request() {
    assert!(
        program(ProgramModel::Application, Some(PhpWorkUnit::Request))
            .is_on_http()
    );
    assert!(!program(ProgramModel::Application, None).is_on_http());

    // An event is a unit of work like a request, and is bounded like one —
    // but it is not HTTP, and a program handling one cannot respond.
    assert!(
        !program(ProgramModel::Application, Some(PhpWorkUnit::Event))
            .is_on_http()
    );

    // A task never receives, which is the whole of what makes it one.
    assert!(!program(ProgramModel::Task, None).is_on_http());
}

/// Both facts `run` used to take as a loose `&str` and a loose `bool`,
/// asserted where they now live. The label is `task`, and PHP reads it as
/// `CLAVIRON_PHP_MODEL` — the third spelling after `dedicated` and
/// `daemon`, and the first one that leaves Rust.
#[test]
fn the_model_names_itself_and_says_whether_work_is_visible() {
    assert_eq!(ProgramModel::Task.as_str(), "task");
    assert_eq!(ProgramModel::Application.as_str(), "application");
    assert!(!ProgramModel::Task.receives_work());
    assert!(ProgramModel::Task.uses_console_output());
    assert!(!ProgramModel::Application.uses_console_output());
    assert!(ProgramModel::Application.receives_work());
}

#[test]
fn only_applications_expose_structured_log_identity() {
    let application = program(ProgramModel::Application, None);
    assert_eq!(application.application_log_scope(), Some(("p", 0)));
    assert_eq!(program(ProgramModel::Task, None).application_log_scope(), None);
}
