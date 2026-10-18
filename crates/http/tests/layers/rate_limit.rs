use super::*;
use std::sync::Barrier;

#[test]
fn simultaneous_requests_preserve_every_increment() {
    let limiter = RateLimiter::new(Duration::from_secs(60), 100);
    let ip = "192.0.2.1".parse().unwrap();
    let barrier = Arc::new(Barrier::new(4));
    let mut counts = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let limiter = limiter.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    (0..32).map(|_| limiter.hit(ip).1).collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    counts.sort_unstable();
    assert_eq!(counts, (1..=128).collect::<Vec<_>>());
}
