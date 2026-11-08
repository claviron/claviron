use std::sync::Arc;

pub fn profile(
    protocol: &str,
    capacity: &str,
) -> Arc<crate::compile::profile::Profile> {
    let yaml = format!(
        "upstreams:\n  example:\n    protocol: {protocol}\n    origins: [tcp://127.0.0.1:1]\n    connection: {{connect_timeout: off}}\n    capacity: {capacity}\n"
    );
    let config: crate::config::UpstreamYaml =
        serde_yaml::from_str(&yaml).unwrap();
    let mut profile = config
        .compile_profile_with_origins(
            "example",
            &config.upstreams["example"],
            &Default::default(),
            None,
            &Default::default(),
        )
        .unwrap();
    let table = crate::state::UpstreamStateTable::memory(1);
    for origin in Arc::make_mut(&mut profile.origins) {
        origin.slot.0 = Some(table.install(&origin.state_key, 0).unwrap());
    }
    Arc::new(profile)
}
