use super::Workers;
use std::num::NonZeroUsize;

#[test]
fn explicit_workers_do_not_query_cpu_availability() {
    for count in [0, 4, u8::MAX] {
        assert_eq!(
            Workers::Count(count)
                .resolve(|| panic!("explicit count must not query CPUs"))
                .unwrap()
                .raw(),
            count
        );
    }
    let _ = crate::Server::new().workers(4);
    let _ = crate::Server::new().workers(Workers::Available);
}

#[test]
fn available_workers_resolve_the_reported_count_without_fallback() {
    assert_eq!(
        Workers::Available
            .resolve(|| Ok(NonZeroUsize::new(12).unwrap()))
            .unwrap()
            .raw(),
        12
    );
    assert!(
        Workers::Available
            .resolve(|| Err(std::io::Error::other("unavailable")))
            .is_err()
    );
    assert!(
        Workers::Available
            .resolve(|| Ok(NonZeroUsize::new(256).unwrap()))
            .is_err()
    );
}
