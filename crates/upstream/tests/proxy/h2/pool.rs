use super::*;
use std::future::{Future, pending};
use std::task::{Context, Poll, Wake, Waker};

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

use crate::proxy::test_profile as fixture;

fn pool(capacity: &str) -> Arc<H2TargetPool> {
    Arc::new(H2TargetPool::new(
        fixture::profile("h2", capacity),
        PoolGeneration(1),
        crate::origin::OriginId(0),
    ))
}

// Admission-only fixture: keep a real Hyper sender alive over isolated memory
// IO and explicitly supply the peer's capacity. No HTTP success is simulated.
async fn connection(
    pool: &Arc<H2TargetPool>,
    id: u64,
    generation: PoolGeneration,
) -> Result<H2Connection, ()> {
    let (io, peer) = tokio::io::duplex(4096);
    let (sender, connection) =
        hyper::client::conn::http2::handshake::<_, _, H2Body>(
            hyper_util::rt::TokioExecutor::new(),
            hyper_util::rt::TokioIo::new(io),
        )
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        let _connection = connection;
        let _peer = peer;
        pending::<()>().await;
    });
    let capacity = Arc::new(super::super::capacity::StreamCapacity::new(
        pool.profile.capacity.max_streams(),
        pool.changed.clone(),
    ));
    capacity.update_peer(8);
    Ok(H2Connection::new(
        id,
        crate::origin::OriginId(0),
        generation,
        sender,
        driver,
        capacity,
        Arc::downgrade(pool),
    ))
}

#[tokio::test]
async fn audit_upstream_warm_h2_grows_at_stream_capacity_and_trims_idle() {
    let pool = pool("{max_live: 2, max_idle: 1, max_streams: 2}");
    let dial = |id, generation| connection(&pool, id, generation);
    let first = pool.acquire(None, &dial).await.unwrap();
    let second = pool.acquire(None, &dial).await.unwrap();
    assert_eq!(first.permit.conn_id, second.permit.conn_id);
    assert!(!pool.is_full(), "one physical connection can still be opened");
    let third = pool.acquire(None, &dial).await.unwrap();
    assert_ne!(first.permit.conn_id, third.permit.conn_id);
    let fourth = pool.acquire(None, &dial).await.unwrap();
    assert_eq!(third.permit.conn_id, fourth.permit.conn_id);
    assert!(pool.is_full());
    assert_eq!(pool.in_flight(), 4);
    assert_eq!(pool.stream_limit(), Some(4));
    let mut fifth = Box::pin(pool.acquire(None, &dial));
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    assert!(fifth.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    assert_eq!(
        wakes.0.load(Ordering::Relaxed),
        0,
        "a full pool must park without self-waking"
    );
    assert_eq!(pool.next_conn_id.load(Ordering::Relaxed), 3);
    let reusable = first.permit.conn_id;
    drop(first);
    assert!(
        wakes.0.load(Ordering::Relaxed) > 0,
        "stream release must wake the registered waiter"
    );
    let fifth = fifth.await.unwrap();
    assert_eq!(fifth.permit.conn_id, reusable);
    drop((second, third, fourth, fifth));
    assert_eq!(pool.in_flight(), 0);
    assert_eq!(pool.connections.load().len(), 1);
    assert_eq!(pool.live.available_permits(), 1);
    pool.close();
    assert_eq!(pool.live.available_permits(), 2);
}

#[tokio::test]
async fn audit_upstream_warm_h2_preconnect_is_one_and_writer_retains_stream() {
    let pool = pool("{max_live: 3, max_idle: 0, max_streams: 1}");
    let dial = |id, generation| connection(&pool, id, generation);
    let warm = pool.ensure_connection(&dial, false).await.unwrap().unwrap();
    let again = pool.ensure_connection(&dial, false).await.unwrap().unwrap();
    assert_eq!(warm.id, again.id);
    assert_eq!(pool.in_flight(), 0);
    drop((warm, again));
    let mut request = pool.acquire(None, &dial).await.unwrap();
    let writer = request.permit.retain_writer();
    drop(request);
    assert_eq!(
        pool.in_flight(),
        1,
        "an early response cannot release the upload's stream"
    );
    assert_eq!(pool.connections.load().len(), 1);
    drop(writer);
    assert_eq!(pool.in_flight(), 0);
    assert!(pool.connections.load().is_empty());
    assert_eq!(pool.live.available_permits(), 3);
    pool.close();
}

#[test]
fn audit_upstream_warm_h2_cancelled_dial_refunds_capacity_and_wakes_waiters() {
    let pool = pool("{max_live: 1}");
    let calls = AtomicUsize::new(0);
    let dial = |_, _| {
        let call = calls.fetch_add(1, Ordering::Relaxed);
        async move {
            if call == 0 {
                pending::<()>().await;
            }
            Err::<H2Connection, ()>(())
        }
    };
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut first = Box::pin(pool.acquire(None, &dial));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    let mut second = Box::pin(pool.acquire(None, &dial));
    assert!(second.as_mut().poll(&mut cx).is_pending());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(pool.live.available_permits(), 0);
    drop(first);
    assert!(
        wakes.0.load(Ordering::Relaxed) > 0,
        "cancelled dial must wake capacity waiters"
    );
    assert_eq!(pool.live.available_permits(), 1);
    assert!(matches!(
        second.as_mut().poll(&mut cx),
        Poll::Ready(Err(H2AcquireError::Connect(())))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    pool.close();
}

#[tokio::test]
async fn audit_upstream_warm_h2_draining_connection_keeps_physical_budget() {
    let pool = pool("{max_live: 1, max_idle: 1, max_streams: 1}");
    let dial = |id, generation| connection(&pool, id, generation);
    let first = pool.acquire(None, &dial).await.unwrap();
    let old_id = first.permit.conn_id;
    pool.clear_connection(old_id).await;
    assert!(pool.connections.load().is_empty());
    assert_eq!(pool.live.available_permits(), 0);
    let mut second = Box::pin(pool.acquire(None, &dial));
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(first);
    let second = second.await.unwrap();
    assert_ne!(old_id, second.permit.conn_id);
    pool.clear_connection(old_id).await;
    assert_eq!(
        pool.connections.load().len(),
        1,
        "late cleanup cannot clear a replacement"
    );
    pool.close();
    assert_eq!(pool.live.available_permits(), 0);
    drop(second);
    assert_eq!(pool.live.available_permits(), 1);
}
