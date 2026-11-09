//! What choosing a Link target costs, and nothing else.
//!
//! An edge chooses a receiver for every exchange by reading the receivers'
//! lane headers in shared memory. Those headers are also where writers claim
//! frame positions and owners record what they consumed, so under traffic the
//! lines being read keep changing under the reader.
//!
//! Two choices are timed over the same lanes:
//!
//! - `scan`: the selection Link made before it filled a home receiver, kept
//!   here as the reference — every lane's admission, work capacity and depth,
//!   collected, sorted, the tie broken by rotation.
//! - `two`: the same reads for two lanes picked at random, the shallower kept;
//!   what `ApplicationClient::choose` now does for unbounded receivers.
//!
//! Each runs beside other selectors doing the same: background threads that
//! choose with the same policy and publish one frame to the lane they chose, as
//! Edge workers do, while each lane's owner consumes its frames. Traffic thus
//! grows with the number of selectors. One selector is the lanes left idle.
//! The timed thread takes no reservation, stream or frame.
//!
//! ```sh
//! cargo bench --all-features --bench link-selection
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;

use claviron::orbit::link::application::{
    Access, Admission, ApplicationSegment, Inbox, LINK_INBOX_KIND,
    LINK_STREAM_KIND,
};
use claviron::orbitive::link::{InboxGeometry, LinkSpec};
use claviron::orbitive::stream::StreamSpec;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

/// Lanes in the table: the largest receiver count plus spare.
const FLEET_CAPACITY: u16 = 64;
/// Receiver counts compared.
const RECEIVERS: [usize; 4] = [1, 8, 16, 40];
/// Selecting threads, the timed one included.
const SELECTORS: [usize; 3] = [1, 8, 16];
/// Threads owning and consuming the receiver lanes.
const OWNERS: usize = 4;
const FRAME: &[u8] = &[0; 16];

/// The former full-scan selection, over `lanes`.
fn scan(
    inbox: &Inbox,
    lanes: &[usize],
    rotation: &AtomicU64,
) -> Option<usize> {
    let mut targets: Vec<_> = lanes
        .iter()
        .copied()
        .filter_map(|lane| candidate(inbox, lane).map(|depth| (lane, depth)))
        .collect();
    targets.sort_by_key(|(_, depth)| *depth);
    let (_, idlest) = targets.first().copied()?;
    let tied = targets.iter().take_while(|(_, depth)| *depth == idlest).count();
    let turn = rotation.fetch_add(1, Ordering::Relaxed) as usize;
    Some(targets[turn % tied].0)
}

/// Two lanes picked at random, the shallower kept.
fn two(
    inbox: &Inbox,
    lanes: &[usize],
    state: &mut u64,
) -> Option<usize> {
    if lanes.len() == 1 {
        return candidate(inbox, lanes[0]).map(|_| lanes[0]);
    }
    let first = (next(state) % lanes.len() as u64) as usize;
    let mut second = (next(state) % (lanes.len() as u64 - 1)) as usize;
    if second >= first {
        second += 1;
    }
    let (first, second) = (lanes[first], lanes[second]);
    match (candidate(inbox, first), candidate(inbox, second)) {
        (Some(a), Some(b)) => Some(if b < a { second } else { first }),
        (Some(_), None) => Some(first),
        (None, Some(_)) => Some(second),
        (None, None) => None,
    }
}

/// A lane's depth when `send_to` would consider it at all.
fn candidate(
    inbox: &Inbox,
    lane: usize,
) -> Option<u64> {
    let admission = inbox.admits(lane).ok()?;
    let bounded = inbox.work_capacity(lane).ok()?.is_some();
    if admission == Admission::Accepted
        || (bounded && admission == Admission::Full)
    {
        inbox.depth(lane).ok()
    } else {
        None
    }
}

/// xorshift64: a choice, not a statistic.
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[derive(Clone, Copy)]
enum Policy {
    Scan,
    Two,
}

impl Policy {
    fn name(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Two => "two",
        }
    }
}

/// Background selectors publishing to the lanes they choose, and the lanes'
/// owners consuming, so the headers the timed selection reads keep changing.
struct Traffic {
    running: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl Traffic {
    fn start(
        segment: &Arc<ApplicationSegment>,
        lanes: &[usize],
        selectors: usize,
        policy: Policy,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let mut threads = Vec::new();
        if selectors <= 1 {
            return Self { running, threads };
        }
        for selector in 1..selectors {
            let segment = Arc::clone(segment);
            let running = Arc::clone(&running);
            let lanes = lanes.to_vec();
            threads.push(std::thread::spawn(move || {
                // Each Edge worker seeds its rotation from its own lane.
                let rotation = AtomicU64::new(selector as u64);
                let mut state = 0x9e37_79b9_7f4a_7c15_u64 ^ selector as u64;
                while running.load(Ordering::Relaxed) {
                    let chosen = match policy {
                        Policy::Scan => {
                            scan(segment.inbox(), &lanes, &rotation)
                        }
                        Policy::Two => two(segment.inbox(), &lanes, &mut state),
                    };
                    if let Some(lane) = chosen {
                        let _ = segment.inbox().write(lane, FRAME);
                    }
                }
            }));
        }
        for owner in 0..OWNERS {
            let segment = Arc::clone(segment);
            let running = Arc::clone(&running);
            // Each lane has exactly one owner reading it.
            let owned: Vec<_> = lanes
                .iter()
                .copied()
                .filter(|lane| lane % OWNERS == owner)
                .collect();
            if owned.is_empty() {
                continue;
            }
            threads.push(std::thread::spawn(move || {
                let mut frame = Vec::new();
                while running.load(Ordering::Relaxed) {
                    let mut took = false;
                    for &lane in &owned {
                        took |= segment
                            .inbox()
                            .read(lane, &mut frame)
                            .unwrap_or(false);
                    }
                    if !took {
                        std::hint::spin_loop();
                    }
                }
            }));
        }
        Self { running, threads }
    }

    fn stop(self) {
        self.running.store(false, Ordering::Relaxed);
        for thread in self.threads {
            thread.join().expect("a traffic thread panicked");
        }
    }
}

fn benchmark(criterion: &mut Criterion) {
    let name = format!("ls{}", std::process::id() % 1_000_000);
    let spec = LinkSpec::new(
        FLEET_CAPACITY,
        LINK_INBOX_KIND,
        InboxGeometry::new(128, 256),
        StreamSpec::new(LINK_STREAM_KIND, 1, 4096),
    );
    let segment = Arc::new(
        ApplicationSegment::for_fleet_with_spec(
            &name,
            spec,
            &Access::OwnerOnly,
        )
        .expect("open the bench's own Link table"),
    );
    let all: Vec<usize> = (0..*RECEIVERS.iter().max().unwrap()).collect();
    for &lane in &all {
        segment
            .inbox()
            .claim_lane(lane, "selection", "bench", 1)
            .expect("claim a receiver lane");
    }

    for selectors in SELECTORS {
        let mut group = criterion
            .benchmark_group(format!("link/selection/selectors-{selectors}"));
        group.warm_up_time(std::time::Duration::from_secs(1));
        group.measurement_time(std::time::Duration::from_secs(3));
        for receivers in RECEIVERS {
            let lanes = &all[..receivers];
            for policy in [Policy::Scan, Policy::Two] {
                let traffic =
                    Traffic::start(&segment, lanes, selectors, policy);
                let rotation = AtomicU64::new(0);
                let mut state = 0x9e37_79b9_7f4a_7c15_u64;
                group.bench_with_input(
                    BenchmarkId::new(policy.name(), receivers),
                    &lanes,
                    |bencher, lanes| {
                        bencher.iter(|| {
                            std::hint::black_box(match policy {
                                Policy::Scan => {
                                    scan(segment.inbox(), lanes, &rotation)
                                }
                                Policy::Two => {
                                    two(segment.inbox(), lanes, &mut state)
                                }
                            })
                        })
                    },
                );
                traffic.stop();
            }
        }
        group.finish();
    }

    segment.unlink().expect("remove the bench's own Link table");
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
