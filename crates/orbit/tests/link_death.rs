//! An application killed with work outstanding, with nobody to report it.

#![cfg(unix)]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use claviron_orbit::link::application::{
    Access, ApplicationBodies, ApplicationClient, ApplicationSegment, Error,
    Exchange,
};
use nix::sys::signal::{Signal, kill};
use nix::sys::wait::waitpid;
use nix::unistd::{ForkResult, fork, pause};

fn exchange() -> Exchange {
    Exchange {
        method: "GET".to_owned(),
        scheme: "http".to_owned(),
        path: "/".to_owned(),
        ..Exchange::default()
    }
}

#[test]
fn a_killed_application_ends_what_waits_on_it_and_refuses_what_follows() {
    let fleet = format!("ld{}", std::process::id());
    let (mut ready, mut joined) = UnixStream::pair().unwrap();
    // SAFETY: the only test in this binary, forked before any runtime exists.
    let child = match unsafe { fork() }.unwrap() {
        ForkResult::Child => {
            let segment = ApplicationSegment::for_fleet(&fleet).unwrap();
            let _lane = segment.join("example-app", "rust", 1).unwrap();
            joined.write_all(&[1]).unwrap();
            loop {
                pause();
            }
        }
        ForkResult::Parent { child } => child,
    };
    ready.read_exact(&mut [0]).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let client = ApplicationClient::join_with_access(
            &fleet,
            "example-edge",
            1,
            &Access::OwnerOnly,
        )
        .unwrap();
        let (_request, _response, reply, _) =
            client.send("example-app", exchange()).unwrap().split();

        kill(child, Signal::SIGKILL).unwrap();

        // The product waits on no clock. This only fails the test instead of
        // hanging it, and the child stays unreaped until it has answered: the
        // exit notification is the only signal.
        let head = tokio::time::timeout(Duration::from_secs(10), reply.head())
            .await
            .expect("a dead application left its exchange waiting");
        assert!(head.is_err());
        match client.send("example-app", exchange()) {
            Err(Error::NoConsumer(_)) => {}
            Err(error) => panic!("refused as {error:?}"),
            Ok(_) => panic!("a request was queued for a dead application"),
        }
    });
    waitpid(child, None).unwrap();
    drop(runtime);
    let segment = ApplicationSegment::for_fleet(&fleet).unwrap();
    let cleanup = segment.join("example-cleanup", "rust", 3).unwrap();
    ApplicationBodies::for_segment(&segment, cleanup.lane, 3)
        .unwrap()
        .unlink()
        .unwrap();
    segment.unlink().unwrap();
}
