use lumenize::{Options, Runner};
mod app;
use app::routes::rust_interceptor;
use app::services::{DemoRoutableService, OrbitalCellService};

// Use mimalloc as the global allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> lumenize::Result<()> {
    Runner::new("octane-app")
        .with_fleet_name(String::from("octane"))
        .with_options(Options::from_args()?)
        .with_services::<(OrbitalCellService, DemoRoutableService)>()
        .with_interceptor(rust_interceptor)
        .run()
        .await
    // .with_laravel_config::<(App,Broadcasting,Logging,Cache,Database,Auth,Session)>("config")
    // .with_h2c(SocketAddr::from(([127, 0, 0, 1], 8080)))
    // .with_h2c_uds("/tmp/octane-app.sock")        // .with_http1(address)
    // .with_http2(address, tls)
    // .with_http3(address, tls)
    // .enable_metrics()
    // .with_max_in_flight(128)
    // .with_max_concurrent_requests(128)
    // .with_request_policy(policy)
    // .with_response_policy(policy)
    // .with_trusted_proxies(networks)
    // .with_tls(certificate, private_key)
    // .with_client_ca(authority)
    // .with_static_folder("public/assets")
    // .try_files()
    // .with_watching(paths)
    //.with_broadcasting("ocatane-echo")
    // .shared_with(Application::from::<Deno>("octane-deno"))
    // .run_fleet(FleetName::from("oap"), WorkerCount::from(4))
    // Alternatively, run though provided Options (clap::Parser + lumenize.yaml);
    // Alternatively, register through .with_bootstrap(app::bootstrap).
    // .with_bootstrap(app::bootstrap)
    // .with_orbit_announce(FleetName::from("edge"))
    // .run_fleet(FleetName::from("oap"), WorkerCount::from(4))
}
