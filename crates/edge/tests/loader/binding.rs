//! A connection's host binding retires when its host moves to another vhost
//! disappears, or replaces its configuration. Polled with a no-op waker: whether
//! the future is ready is the whole question, and no clock decides it.

use std::future::Future;
use std::pin::{Pin, pin};
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Waker};

use claviron_types::Host;

use super::*;

fn host(name: &str) -> Host {
    Host::from_str(name).expect("valid host")
}

fn ready(future: Pin<&mut impl Future<Output = ()>>) -> bool {
    future.poll(&mut Context::from_waker(Waker::noop())).is_ready()
}

/// `a.test` with alias `www.a.test`, and `b.test`, published once.
fn resolver() -> Arc<VhostResolver> {
    let table: VhostTable = serde_yaml::from_str(
        r#"
a.test:
  aliases: [www.a.test]
  root: public
b.test:
  root: public
"#,
    )
    .expect("parse vhost table");
    let resolver = Arc::new(VhostResolver::default());
    resolver.publish(table).expect("publish host table");
    resolver
}

fn bound(
    resolver: &Arc<VhostResolver>,
    name: &str,
    canonical: &str,
) -> HostBinding {
    resolver.bind(Arc::new(host(name)), Arc::new(host(canonical)))
}

#[test]
fn moving_the_alias_to_another_vhost_retires_the_binding() {
    let resolver = resolver();
    let mut binding = bound(&resolver, "www.a.test", "a.test");
    let mut retired = pin!(binding.retired());
    assert!(!ready(retired.as_mut()), "an unchanged table holds the binding");

    resolver
        .upsert_alias(host("www.a.test"), host("b.test"))
        .expect("move alias");

    assert!(ready(retired.as_mut()), "the moved alias retires the binding");
}

#[test]
fn removing_the_host_retires_the_binding() {
    let resolver = resolver();
    let mut binding = bound(&resolver, "a.test", "a.test");
    let mut retired = pin!(binding.retired());
    assert!(!ready(retired.as_mut()));

    assert!(resolver.delete_host(&host("a.test")));

    assert!(ready(retired.as_mut()), "a host that is gone retires the binding");
}

#[test]
fn republishing_the_same_configuration_keeps_the_binding() {
    let resolver = resolver();
    let config = resolver
        .snapshot()
        .canonical_config(&host("a.test"))
        .expect("a.test is configured");
    let mut binding = bound(&resolver, "www.a.test", "a.test");
    let mut retired = pin!(binding.retired());
    assert!(!ready(retired.as_mut()));

    // The exact same config Arc and alias mapping are retained.
    resolver.upsert_host(host("a.test"), config);

    assert!(!ready(retired.as_mut()), "same vhost, same binding");
}

#[test]
fn an_unrelated_change_keeps_the_binding() {
    let resolver = resolver();
    let mut binding = bound(&resolver, "www.a.test", "a.test");
    let mut retired = pin!(binding.retired());
    assert!(!ready(retired.as_mut()));

    resolver
        .upsert_alias(host("www.b.test"), host("b.test"))
        .expect("add alias elsewhere");

    assert!(
        !ready(retired.as_mut()),
        "another host's alias does not move this one"
    );
}

#[test]
fn a_move_published_before_the_first_poll_is_not_missed() {
    let resolver = resolver();
    let mut binding = bound(&resolver, "www.a.test", "a.test");

    resolver
        .upsert_alias(host("www.a.test"), host("b.test"))
        .expect("move alias");

    let mut retired = pin!(binding.retired());
    assert!(ready(retired.as_mut()), "the first poll checks the current table");
}
