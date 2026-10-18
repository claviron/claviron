use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

struct ErrorSink {
    calls: AtomicUsize,
    panic_filter: bool,
}

impl PhpObserver for ErrorSink {
    fn interests(&self) -> PhpObserverInterests {
        PhpObserverInterests::errors()
    }

    fn should_observe_error(
        &self,
        error_type: ErrorType,
    ) -> bool {
        assert!(!self.panic_filter, "filter panic");
        error_type == ErrorType::Warning
    }

    fn on_error(
        &self,
        _error: &ErrorInfo<'_>,
    ) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn error_dispatcher_filters_each_sink() {
    let sink =
        Arc::new(ErrorSink { calls: AtomicUsize::new(0), panic_filter: false });
    let slot = Arc::new(ObserverSlot::new("test", sink.clone()));
    let dispatcher = ErrorDispatcher { slots: vec![slot] };
    assert!(dispatcher.should_observe(ErrorType::Warning));
    assert!(!dispatcher.should_observe(ErrorType::Notice));

    let info = ErrorInfo {
        error_type: ErrorType::Warning,
        filename: Some("test.php"),
        lineno: 7,
        message: "warning",
    };
    dispatcher.on_error(&info);
    assert_eq!(sink.calls.load(Ordering::Relaxed), 1);
}

#[test]
fn panicking_sink_is_disabled() {
    let sink =
        Arc::new(ErrorSink { calls: AtomicUsize::new(0), panic_filter: true });
    let slot = Arc::new(ObserverSlot::new("panic-test", sink));
    let dispatcher = ErrorDispatcher { slots: vec![slot.clone()] };
    assert!(!dispatcher.should_observe(ErrorType::Warning));
    assert!(!slot.enabled.load(Ordering::Acquire));
    assert!(!dispatcher.should_observe(ErrorType::Warning));
}

#[test]
fn interest_constructors_are_narrow() {
    assert_eq!(
        PhpObserverInterests::fcalls(),
        PhpObserverInterests { fcalls: true, errors: false, exceptions: false }
    );
    assert_eq!(
        PhpObserverInterests::all(),
        PhpObserverInterests { fcalls: true, errors: true, exceptions: true }
    );
}
