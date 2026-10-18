use std::sync::Arc;

use orbitive::NodeId;

use super::*;

#[tokio::test]
async fn carries_multi_chunk_publications_between_fleet_nodes() {
    let name =
        Box::leak(format!("skt{:x}", std::process::id()).into_boxed_str());
    let writer = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).unwrap());
    let reader = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).unwrap());
    writer.shm_ring::<PublicationRecord>().unwrap().reset();
    writer.shm_ring::<PayloadRecord>().unwrap().reset();

    let writer = FleetFanout::new(writer);
    let reader = FleetFanout::new(Arc::clone(&reader));
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    let event_fd = match reader.event_fd() {
        Ok(event_fd) => {
            // SAFETY: RingEventFd owns its stable descriptor until drop;
            // the test only drains it and never replaces the inner value.
            Some(
                unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
                    .unwrap(),
            )
        }
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
        Err(error) => panic!("descriptor readiness failed: {error}"),
    };
    let mut cursor = reader.cursor_at_head();
    let event = PublishedEvent {
        name: "wallet.updated".to_owned(),
        data: "x".repeat(PAYLOAD_CHUNK_BYTES + 17),
        channels: vec!["wallet".to_owned()],
        socket_id: None,
    };

    writer.publish("laravel", &event).unwrap();

    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    if let Some(event_fd) = &event_fd {
        let mut ready = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            event_fd.readable(),
        )
        .await
        .expect("descriptor publication did not wake reader")
        .unwrap();
        event_fd.get_ref().drain().unwrap();
        ready.clear_ready();
    }

    let publications = reader.poll(&mut cursor);
    assert!(matches!(
        publications.as_slice(),
        [FleetInbound {
            source_node: 0,
            publication: FleetPublication::Event { application, event: observed }
        }]
            if application == "laravel" && observed == &event
    ));
    reader.fleet.shm_ring::<PublicationRecord>().unwrap().unlink().unwrap();
    reader.fleet.shm_ring::<PayloadRecord>().unwrap().unlink().unwrap();
}
