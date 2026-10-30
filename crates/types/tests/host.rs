use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

use claviron_types::{Host, Sni};

// Count only the current test thread and only while parsing. This allocator
// belongs to this integration-test binary, never to the library or its users.
thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct ParseAllocator;

#[global_allocator]
static ALLOCATOR: ParseAllocator = ParseAllocator;

fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(current) = count.get() {
            count.set(Some(current + 1));
        }
    });
}

// SAFETY: Every operation forwards the caller's unchanged pointer/layout to
// System. Tracking uses a constant-initialized Cell and does not allocate.
unsafe impl GlobalAlloc for ParseAllocator {
    unsafe fn alloc(
        &self,
        layout: Layout,
    ) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(
        &self,
        layout: Layout,
    ) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        size: usize,
    ) -> *mut u8 {
        record_allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
    ) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

fn dns(name: &str) -> Host {
    Host::Dns(Sni(Arc::from(name)))
}

#[test]
fn host_parse_preserves_variants_and_normalization() {
    let ipv4 = Host::Ip(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)));
    let ipv6 = Host::Ip(IpAddr::V6(Ipv6Addr::new(
        0x2001, 0xdb8, 0, 0, 0, 0, 0, 0xabcd,
    )));
    for (raw, expected) in [
        ("api.example.test", dns("api.example.test")),
        ("API.Example.TEST...", dns("api.example.test")),
        (" \t\0API.Example.TEST.\x7f", dns("api.example.test")),
        ("*.EXAMPLE.TEST.", dns("*.example.test")),
        ("dead.beef.example.test", dns("dead.beef.example.test")),
        ("1e2.0.0.1.example.test", dns("1e2.0.0.1.example.test")),
        ("BARE-NAME...", Host::Single(Arc::from("bare-name"))),
        ("123", Host::Single(Arc::from("123"))),
        ("192.0.2.1", ipv4.clone()),
        (" \t192.0.2.1\r\n", ipv4.clone()),
        ("[192.0.2.1]", ipv4),
        ("2001:DB8::ABCD", ipv6.clone()),
        ("[2001:DB8::ABCD]", ipv6),
    ] {
        assert_eq!(Host::from_str(raw).unwrap(), expected, "{raw:?}");
    }
}

#[test]
fn host_parse_preserves_rejections_and_error_normalization() {
    for (raw, expected) in [
        ("", "invalid host alias: <empty>"),
        (" \t\0...\r\n", "invalid host alias: <empty>"),
        ("192.0.2.1.", "invalid host alias: 192.0.2.1"),
        ("[2001:DB8::1].", "invalid host alias: [2001:db8::1]"),
        ("[EXAMPLE.TEST]", "invalid host alias: [example.test]"),
        ("999.0.2.1", "invalid host alias: 999.0.2.1"),
        ("01.2.3.4", "invalid host alias: 01.2.3.4"),
        ("API..TEST.", "invalid host alias: api..test"),
        ("-API.TEST", "invalid host alias: -api.test"),
        ("API-.TEST", "invalid host alias: api-.test"),
        ("UNDER_SCORE", "invalid host alias: under_score"),
        ("API TEST", "invalid host alias: api test"),
        ("**.EXAMPLE.TEST", "invalid host alias: **.example.test"),
        ("Ä.EXAMPLE.TEST", "invalid host alias: Ä.example.test"),
        ("[2001:DB8::1%ETH0]", "invalid host alias: [2001:db8::1%eth0]"),
    ] {
        assert_eq!(Host::from_str(raw), Err(expected.to_owned()), "{raw:?}");
    }

    let label = "a".repeat(63);
    assert_eq!(
        Host::from_str(&label).unwrap(),
        Host::Single(Arc::from(label.as_str()))
    );
    assert!(Host::from_str(&"a".repeat(64)).is_err());
    let longest = format!("{label}.{label}.{label}.{}", "a".repeat(61));
    assert_eq!(Host::from_str(&longest).unwrap(), dns(&longest));
    // The existing SNI rule limits the name after the wildcard prefix.
    let wildcard = format!("*.{longest}");
    assert_eq!(Host::from_str(&wildcard).unwrap(), dns(&wildcard));
    assert!(Host::from_str(&format!("{longest}a")).is_err());
}

#[test]
fn host_parse_allocations_are_limited_to_normalization_and_owned_identity() {
    for (raw, expected) in [
        ("192.0.2.1", 0),
        ("[192.0.2.1]", 0),
        ("2001:DB8::ABCD", 0),
        ("[2001:DB8::ABCD]", 0),
        ("api.example.test", 1),
        ("*.example.test...", 1),
        ("bare-name", 1),
        ("API.Example.TEST", 2),
        ("BARE-NAME", 2),
    ] {
        ALLOCATIONS.with(|count| count.set(Some(0)));
        let parsed = Host::from_str(std::hint::black_box(raw));
        let allocations =
            ALLOCATIONS.with(|count| count.replace(None).unwrap());
        assert!(parsed.is_ok(), "{raw:?}");
        assert_eq!(allocations, expected, "{raw:?}");
    }
}
