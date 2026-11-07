use std::ptr;

use super::*;
use crate::abi::{Host, Registrar, Service, Slice};
use crate::config::ServiceConfig;
use continuo::fleet::{Dispatch, Scope};

unsafe extern "C" fn run(
    _: *mut abi::Cancel,
    _: *mut c_void,
) -> u32 {
    abi::OK
}

fn service() -> Service {
    Service {
        struct_size: size_of::<Service>() as u32,
        context: ptr::null_mut(),
        run: Some(run),
    }
}

unsafe extern "C" fn registers_indexer(
    host: *const Host,
    registrar: *mut Registrar,
) -> u32 {
    let service = service();
    unsafe {
        ((*host).register_service)(registrar, Slice::of(b"indexer"), &service)
    }
}

unsafe extern "C" fn registers_twice(
    host: *const Host,
    registrar: *mut Registrar,
) -> u32 {
    unsafe {
        registers_indexer(host, registrar);
        registers_indexer(host, registrar)
    }
}

unsafe extern "C" fn fails(
    _: *const Host,
    _: *mut Registrar,
) -> u32 {
    abi::INVALID_ARGUMENT
}

fn declared(names: &[&str]) -> ModulesConfig {
    ModulesConfig {
        modules: Vec::new(),
        applications: Default::default(),
        services: names
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    ServiceConfig {
                        scope: Scope::Worker,
                        dispatch: Dispatch::Each,
                        dispatch_policy: None,
                    },
                )
            })
            .collect(),
    }
}

fn collected(
    config: &ModulesConfig,
    entries: &[abi::RegisterFn],
) -> Result<Modules, String> {
    let registered = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let name = format!("lib{index}");
            register(*entry, &name).map(|done| (name, None, done))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ModuleProvider::collect(config, registered, false)
        .map_err(|error| error.to_string())
}

#[test]
fn what_is_registered_and_declared_runs_under_its_declared_placement() {
    let modules =
        collected(&declared(&["indexer"]), &[registers_indexer]).ok().unwrap();
    assert_eq!(modules.services.len(), 1);
    assert_eq!(modules.services[0].name(), "indexer");
}

/// Nothing runs on a value nobody chose, and a typo does not pass silently.
#[test]
fn every_mismatch_or_conflict_stops_boot() {
    let undeclared =
        collected(&declared(&[]), &[registers_indexer]).err().unwrap();
    assert!(undeclared.contains("does not declare"), "{undeclared}");

    let unregistered = collected(&declared(&["indexr"]), &[]).err().unwrap();
    assert!(unregistered.contains("no module registered"), "{unregistered}");

    let twice =
        collected(&declared(&["indexer"]), &[registers_twice]).err().unwrap();
    assert!(twice.contains("registered twice"), "{twice}");

    let two_libraries = collected(
        &declared(&["indexer"]),
        &[registers_indexer, registers_indexer],
    )
    .err()
    .unwrap();
    assert!(two_libraries.contains("more than one module"), "{two_libraries}");

    let failing = collected(&declared(&[]), &[fails]).err().unwrap();
    assert!(failing.contains("status 2"), "{failing}");
}

#[test]
fn modules_yaml_declares_everything_and_refuses_what_it_does_not_know() {
    let parsed: ModulesConfig = serde_yaml::from_str(
        "modules: [lib/libshop.so]\nservices:\n  indexer:\n    scope: workers\n    dispatch: one\n    dispatch_policy: {capacity: 1, lease_ttl: 20s, renewal_interval: 5s}\n",
    )
    .unwrap();
    assert_eq!(parsed.services["indexer"].dispatch, Dispatch::One);

    // Placement is not defaulted.
    assert!(
        serde_yaml::from_str::<ModulesConfig>(
            "modules: []\nservices: {indexer: {dispatch: each}}\n"
        )
        .is_err()
    );
    assert!(
        serde_yaml::from_str::<ModulesConfig>("modules: []\nextra: 1\n")
            .is_err()
    );
}
