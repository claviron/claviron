use std::fs;
use std::hash::{BuildHasher, RandomState};
use std::hint::black_box;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroUsize;
use std::process::{self, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use claviron::orbit::cache::{CacheRuntime, CacheService};
use claviron::orbit::{Fleet, NodeId};
use claviron::orbitive::core::shm::fleet_lock_path;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use moka::sync::Cache;
use tokio::runtime::Builder;

const KEYS: usize = 1024;
const TTL: Duration = Duration::from_secs(30 * 60);
const COUNTRY: &str = "DE";

// Construct before joining, so all mapped handles drop before the guard.
struct BenchFleet(String);

impl BenchFleet {
    fn new() -> Self {
        let output = Command::new("orbit")
            .args(["fleets", "--names"])
            .output()
            .expect("install orbitive-cli before running geoip-cache");
        assert!(output.status.success(), "orbit fleets failed");
        let existing = String::from_utf8(output.stdout).unwrap();
        let hash = RandomState::new().hash_one((process::id(), Instant::now()));
        let name = format!("gc{:06x}", hash & 0xff_ffff);
        assert!(!existing.lines().any(|fleet| fleet == name));
        Self(name)
    }

    fn store(
        &self,
        capacity: u16,
        node: u16,
    ) -> CacheRuntime {
        let fleet =
            Fleet::join_shm_as(self.0.as_str(), capacity, NodeId::new(node))
                .expect("join benchmark SHM fleet");
        CacheService::from_fleet(Arc::new(fleet))
            .expect("open SHM cache")
            .register_store("geoip", NonZeroUsize::new(KEYS).unwrap())
            .expect("register GeoIP store")
    }
}

impl Drop for BenchFleet {
    fn drop(&mut self) {
        let cleanup = Command::new("orbit")
            .args(["clear", &self.0, "--all", "--yes"])
            .status()
            .and_then(|status| {
                if !status.success() {
                    return Err(io::Error::other(format!(
                        "orbit clear: {status}"
                    )));
                }
                match fs::remove_file(fleet_lock_path(&self.0)) {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        Ok(())
                    }
                    result => result,
                }
            });
        if let Err(error) = cleanup {
            eprintln!("Could not clean benchmark fleet {}: {error}", self.0);
            assert!(std::thread::panicking(), "benchmark SHM cleanup failed");
        }
    }
}

fn geoip_cache(c: &mut Criterion) {
    let ips = (0..KEYS)
        .map(|index| IpAddr::V4(Ipv4Addr::from(0xc000_0000 + index as u32)))
        .collect::<Vec<_>>();
    let keys = ips.iter().map(ToString::to_string).collect::<Vec<_>>();
    let miss_ip = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1));
    let miss_key = miss_ip.to_string();
    let moka =
        Cache::builder().max_capacity(KEYS as u64).time_to_live(TTL).build();
    let code: Arc<str> = Arc::from(COUNTRY);
    for &ip in &ips {
        moka.insert(ip, code.clone());
    }
    moka.run_pending_tasks();
    assert!(ips.iter().all(|ip| moka.get(ip).as_deref() == Some(COUNTRY)));

    let mut group = c.benchmark_group("geoip-cache");
    group.bench_function(BenchmarkId::new("hit", "moka"), |b| {
        let mut ips = ips.iter().cycle();
        b.iter(|| moka.get(black_box(ips.next().unwrap())));
    });
    group.bench_function(BenchmarkId::new("miss", "moka"), |b| {
        b.iter(|| moka.get(black_box(&miss_ip)));
    });

    let runtime = Builder::new_current_thread().build().unwrap();
    for (capacity, peer, label) in [
        (2, false, "shm-one"),
        (2, true, "shm-peer"),
        (40, true, "shm-peer-40"),
    ] {
        let fleet = BenchFleet::new();
        let (reader, writer) = {
            let _entered = runtime.enter();
            (fleet.store(capacity, 0), peer.then(|| fleet.store(capacity, 1)))
        };
        let writer = writer.as_ref().unwrap_or(&reader);
        runtime.block_on(async {
            for key in &keys {
                writer.put(key, COUNTRY.as_bytes(), Some(TTL)).await.unwrap();
                // Consume peer mutations during setup, independent of ring capacity.
                let value = reader.get(key).await.unwrap();
                assert_eq!(value.as_deref(), Some(COUNTRY.as_bytes()));
            }
            for key in &keys {
                assert_eq!(
                    reader.get(key).await.unwrap().as_deref(),
                    Some(COUNTRY.as_bytes())
                );
            }
            assert!(reader.get(&miss_key).await.unwrap().is_none());
        });
        assert!(
            Command::new("orbit")
                .args(["list", &fleet.0])
                .status()
                .unwrap()
                .success()
        );

        group.bench_function(
            BenchmarkId::new("hit", format!("{label}-l1")),
            |b| {
                let mut keys = keys.iter().cycle();
                b.iter(|| {
                    reader
                        .read_local(black_box(keys.next().unwrap().as_bytes()))
                });
            },
        );
        group.bench_function(
            BenchmarkId::new("hit", format!("{label}-async")),
            |b| {
                let mut keys = keys.iter().cycle();
                b.to_async(&runtime)
                    .iter(|| reader.get(black_box(keys.next().unwrap())));
            },
        );
        group.bench_function(
            BenchmarkId::new("miss", format!("{label}-async")),
            |b| {
                b.to_async(&runtime).iter(|| reader.get(black_box(&miss_key)));
            },
        );
        group.bench_function(
            BenchmarkId::new("hit", format!("{label}-sync")),
            |b| {
                let mut keys = keys.iter().cycle();
                b.iter(|| reader.get_sync(black_box(keys.next().unwrap())));
            },
        );
        group.bench_function(
            BenchmarkId::new("hit", format!("{label}-sync-ip")),
            |b| {
                let mut ips = ips.iter().cycle();
                b.iter(|| {
                    reader.get_sync(&black_box(ips.next().unwrap()).to_string())
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, geoip_cache);
criterion_main!(benches);
