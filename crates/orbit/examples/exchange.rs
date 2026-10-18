//! An edge worker, reduced to the part that matters: one exchange over shared
//! memory, with no socket anywhere.
//!
//! This is what `claviron-edge` will do inside a request handler. It is here
//! as an example because an application process has to be reachable before the
//! edge knows how to reach it, and this is what proves it:
//!
//! ```sh
//! cargo run --example exchange -p claviron-orbit -- apps example-app /hello/world
//! cargo run --example exchange -p claviron-orbit -- --lanes apps
//! ```
//!
//! It joins the selected fleet, takes a lane of its own for the reply,
//! reads the lanes serving the application, picks the idlest, writes a request
//! and waits. Both ends select the same namespace; the application name is
//! advertised inside it, not used as the physical SHM address.

use claviron_orbit::transport::{
    Exchange, HttpVersion, TransportClient, TransportSegment,
};
use tokio::io::AsyncReadExt;

/// Whether the process holding a lane still exists.
///
/// A report, not a protocol: signal 0 checks for a process without sending
/// anything, and `EPERM` means it is there under another user. A process id
/// can be reused, so this answers "is that pid taken" rather than "is that
/// owner alive" — enough to tell a live fleet from what a crash left behind,
/// and not enough to fail a request on.
fn alive(pid: u32) -> bool {
    let present = unsafe { libc::kill(pid as libc::pid_t, 0) == 0 };
    present
        || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let mut fleet_name = arguments.next().ok_or(
        "usage: exchange [--lanes] <fleet_name> [<application> [<path>]]",
    )?;
    let mode = if fleet_name.starts_with("--") {
        let mode = fleet_name;
        fleet_name = arguments.next().ok_or("a fleet_name is required")?;
        Some(mode)
    } else {
        None
    };
    let segment = TransportSegment::for_fleet(&fleet_name)?;
    let inbox = segment.inbox();
    #[allow(clippy::print_literal)]
    // `--lanes` reads the table instead of writing to it. Membership is a
    // read here, so this is the whole of what an edge knows about who is
    // serving. A lane whose owner died stays listed until the next process
    // joins the fleet and takes it back.
    if mode.as_deref() == Some("--lanes") {
        println!(
            "{:>4}  {:>8}  {:>5}  {:>11}  {:>5}  {:>7}  {}",
            "lane",
            "owner",
            "alive",
            "incarnation",
            "depth",
            "runtime",
            "application"
        );
        for lane in 0..inbox.lanes() {
            if inbox.consumers(lane)? == 0 {
                continue;
            }
            let owner = inbox.owner(lane)?;
            println!(
                "{lane:>4}  {:>8}  {:>5}  {:>11}  {:>5}  {:>7}  {}",
                owner.map_or_else(|| "—".to_owned(), |pid| pid.to_string()),
                owner.map_or("?", |pid| if alive(pid) { "yes" } else { "NO" }),
                inbox.incarnation(lane)?,
                inbox.depth(lane)?,
                inbox.role(lane)?.unwrap_or_else(|| "—".to_owned()),
                inbox.service(lane)?.unwrap_or_else(|| "—".to_owned())
            );
        }
        return Ok(());
    }

    let application = arguments.next().ok_or("an application is required")?;
    let path = arguments.next().unwrap_or_else(|| "/".to_owned());

    // Everything an edge worker does, through the same client it uses: take a
    // lane for the replies, read the lanes serving the application, choose,
    // write.
    let client = TransportClient::join(&fleet_name, "edge-cli", 1)?;
    println!(
        "lanes serving '{application}': {:?}",
        inbox.targets(&application)
    );
    if !client.serves(&application) {
        client.leave();
        return Err(format!(
            "nothing serves '{application}' in fleet '{fleet_name}'"
        )
        .into());
    }

    let request = Exchange {
        method: "GET".to_owned(),
        scheme: "http".to_owned(),
        version: HttpVersion::Http11,
        path: path.clone(),
        raw_path: path.into_bytes(),
        headers: vec![(b"host".to_vec(), b"example.test".to_vec())],
        client: Some(([127, 0, 0, 1], 54321).into()),
        server: Some(([127, 0, 0, 1], 443).into()),
        // The caller fills the rest: correlation, its own lane, the ticket.
        ..Default::default()
    };

    // A GET has nothing to send, so the request direction is ended before the
    // application is told about it. The slot stays open: it belongs to the
    // exchange, not to the request.
    let pending = client.send(&application, request, false)?;
    let (_request_body, mut response_body, reply) = pending.split();

    let head = reply.head().await?;
    println!("← {} ({} headers)", head.status, head.headers.len());
    for (name, value) in &head.headers {
        println!(
            "  {}: {}",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(value)
        );
    }

    let mut body = Vec::new();
    response_body.read_to_end(&mut body).await?;
    println!("{}", String::from_utf8_lossy(&body));

    client.leave();
    Ok(())
}
