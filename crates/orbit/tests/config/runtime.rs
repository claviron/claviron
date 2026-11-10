use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct Warnings(Arc<AtomicUsize>);
impl tracing::Subscriber for Warnings {
    fn enabled(
        &self,
        _: &tracing::Metadata<'_>,
    ) -> bool {
        true
    }
    fn new_span(
        &self,
        _: &tracing::span::Attributes<'_>,
    ) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(
        &self,
        _: &tracing::span::Id,
        _: &tracing::span::Record<'_>,
    ) {
    }
    fn record_follows_from(
        &self,
        _: &tracing::span::Id,
        _: &tracing::span::Id,
    ) {
    }
    fn event(
        &self,
        event: &tracing::Event<'_>,
    ) {
        if *event.metadata().level() == tracing::Level::WARN {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn enter(
        &self,
        _: &tracing::span::Id,
    ) {
    }
    fn exit(
        &self,
        _: &tracing::span::Id,
    ) {
    }
}

fn owner(
    path: PathBuf,
    nested: bool,
) -> OrbitConfiguration {
    OrbitConfiguration {
        source: Mutex::new(Source {
            path,
            nested,
            absent: OrbitConfig::default(),
            fallback: None,
            resolved: None,
            programmatic: false,
        }),
        applied: Mutex::new(Applied::default()),
        capacity: Arc::new(LinkCapacity::new(None)),
        report_changes: true,
    }
}

#[tokio::test]
async fn geometry_warns_once_while_snapshot_and_manual_application_do_not_warn()
{
    let this = owner("unused.yaml".into(), false);
    let initial = OrbitConfig {
        link: Some(super::super::LinkConfig::default()),
        ..OrbitConfig::default()
    };
    this.configure(&initial).await.unwrap();
    let mut changed = initial.clone();
    changed.link.as_mut().unwrap().geometry.inbox_capacity /= 2;
    changed.link.as_mut().unwrap().geometry.lane_capacity /= 2;
    changed.link.as_mut().unwrap().max_in_flight = Some(1);
    let warnings = Warnings(Arc::new(AtomicUsize::new(0)));
    tracing::subscriber::with_default(warnings.clone(), || {
        this.apply_change(&changed, true).unwrap();
        this.apply_change(&changed, true).unwrap();
        this.apply_change(&changed, false).unwrap();
        assert_eq!(warnings.0.load(Ordering::SeqCst), 1);
        // Observing the frozen shape does not publish a config or log.
        assert_eq!(
            this.capacity.geometry().unwrap(),
            initial.link.as_ref().unwrap().geometry
        );
        assert_eq!(warnings.0.load(Ordering::SeqCst), 1);
        this.apply_change(&initial, true).unwrap();
        this.apply_change(&changed, true).unwrap();
        assert_eq!(warnings.0.load(Ordering::SeqCst), 2);
    });
}

#[tokio::test]
async fn explicit_reload_of_a_new_geometry_only_logs_debug() {
    let this = owner("unused.yaml".into(), false);
    let config = OrbitConfig {
        link: Some(super::super::LinkConfig::default()),
        ..OrbitConfig::default()
    };
    this.configure(&config).await.unwrap();
    let mut changed = config;
    changed.link.as_mut().unwrap().geometry.inbox_capacity /= 2;
    changed.link.as_mut().unwrap().geometry.lane_capacity /= 2;
    let warnings = Warnings(Arc::new(AtomicUsize::new(0)));
    tracing::subscriber::with_default(warnings.clone(), || {
        this.apply_change(&changed, false).unwrap()
    });
    assert_eq!(warnings.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn runner_section_is_read_with_the_owners_environment_and_without_boot() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        "name: example\norbit:\n  link:\n    max_in_flight: 2\nengine: {}\n",
    )
    .unwrap();
    let this = owner(file.path().to_owned(), true);
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    reader.initialize_owners(&[&this]).await.unwrap();
    let snapshot = reader.snapshot::<OrbitConfiguration>().unwrap();
    assert_eq!(snapshot.link.as_ref().unwrap().max_in_flight, Some(2));
    this.configure(&snapshot).await.unwrap();
    assert_eq!(this.capacity.geometry().unwrap(), LinkGeometry::default());
    assert_eq!(this.capacity.limit().unwrap(), Some(2));
}

#[tokio::test]
async fn runner_unbounded_admission_survives_watch_and_manual_reload() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "orbit: {link: {}}\n").unwrap();
    let this = owner(file.path().to_owned(), true);
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    reader.initialize_owners(&[&this]).await.unwrap();
    let snapshot = reader.snapshot::<OrbitConfiguration>().unwrap();
    this.configure(&snapshot).await.unwrap();
    assert_eq!(this.capacity.limit().unwrap(), None);
    this.apply_change(&snapshot, true).unwrap();
    this.apply_change(&snapshot, false).unwrap();
    assert_eq!(this.capacity.limit().unwrap(), None);
}

#[tokio::test]
async fn admission_mode_changes_wait_for_restart_but_positive_bounds_update() {
    for initial_limit in [None, Some(1)] {
        let this = owner("unused.yaml".into(), false);
        let initial = OrbitConfig {
            link: Some(super::super::LinkConfig {
                max_in_flight: initial_limit,
                ..Default::default()
            }),
            ..OrbitConfig::default()
        };
        this.configure(&initial).await.unwrap();
        let mut changed = initial.clone();
        changed.link.as_mut().unwrap().max_in_flight =
            if initial_limit.is_some() { None } else { Some(1) };
        let warnings = Warnings(Arc::new(AtomicUsize::new(0)));
        tracing::subscriber::with_default(warnings.clone(), || {
            this.apply_change(&changed, true).unwrap();
            this.apply_change(&changed, true).unwrap();
            this.apply_change(&changed, false).unwrap();
        });
        assert_eq!(warnings.0.load(Ordering::SeqCst), 1);
        assert_eq!(this.capacity.limit().unwrap(), initial_limit);
        this.apply_change(&initial, true).unwrap();
        assert_eq!(this.capacity.limit().unwrap(), initial_limit);
        if initial_limit.is_some() {
            changed.link.as_mut().unwrap().max_in_flight = Some(2);
            this.apply_change(&changed, true).unwrap();
            assert_eq!(this.capacity.limit().unwrap(), Some(2));
        }
    }
}

#[tokio::test]
async fn resolved_programmatic_capacity_remains_pinned_across_file_changes() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "orbit: {link: {max_in_flight: 2}}\n").unwrap();
    let this = owner(file.path().to_owned(), true);
    let mut initial = OrbitConfig {
        link: Some(super::super::LinkConfig::default()),
        ..OrbitConfig::default()
    };
    initial.link.as_mut().unwrap().max_in_flight = Some(3);
    this.source.lock().unwrap().resolved = Some(initial.clone());
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let startup = this.read_config(&reader).await.unwrap();
    assert_eq!(startup, initial);
    this.configure(&startup).await.unwrap();
    std::fs::write(file.path(), "orbit: {link: {max_in_flight: 1}}\n").unwrap();
    let candidate = this.read_config(&reader).await.unwrap();
    assert_eq!(candidate.link.unwrap().max_in_flight, Some(3));
}
