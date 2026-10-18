use std::collections::HashMap;
use std::str::FromStr;

use claviron_types::Host;

use super::*;

#[test]
fn apply_db_aliases_skips_invalid_alias_hosts() {
    let table: VhostTable = serde_yaml::from_str(
        r#"
claviron.dev:
  root: public
"#,
    )
    .expect("parse vhost table");
    let mut snapshot = VhostSnapshot::from_config(table);

    let mut aliases = HashMap::new();
    aliases.insert(
        "claviron.dev".to_string(),
        vec!["www.claviron.dev".to_string(), String::new(), "   ".to_string()],
    );

    snapshot
        .apply_db_aliases(aliases)
        .expect("invalid aliases should be skipped");
    let table = snapshot.into_table();

    assert_eq!(table.host_count(), 1);
    assert_eq!(table.alias_count(), 1);

    let alias = Host::from_str("www.claviron.dev").expect("parse valid alias");
    let canonical = Host::from_str("claviron.dev").expect("parse canonical");
    assert_eq!(table.resolve(&alias).expect("alias resolves").0, canonical);
}
