//! Compile route-local directory policies into the shared file mount view.

use super::{PathPrefix, RouteMapping, Value, key};
use std::path::{Component, PathBuf};

pub(super) fn compile(
    prefix: &PathPrefix,
    mapping: &RouteMapping,
    source: Value,
) -> Result<Vec<Value>, String> {
    let directories = source.as_sequence().ok_or_else(|| {
        format!("route '{prefix}' statics must be a list of directories")
    })?;
    let mut compiled = Vec::new();
    let mut prefixes = Vec::new();
    for directory in directories {
        let mut declaration =
            directory.as_mapping().cloned().ok_or_else(|| {
                format!("route '{prefix}' statics entries must be mappings")
            })?;
        for name in declaration.keys() {
            if !matches!(name.as_str(), Some("dir" | "rules" | "none_found")) {
                return Err(format!(
                    "route '{prefix}' statics accepts dir, rules and none_found; URL paths are derived from dir"
                ));
            }
        }
        let directory: PathBuf = serde_yaml::from_value(
            declaration.get(key("dir")).cloned().ok_or_else(|| {
                format!("route '{prefix}' statics entry requires dir")
            })?,
        )
        .map_err(|error| format!("route '{prefix}' statics dir: {error}"))?;
        let text = directory.to_str().ok_or_else(|| {
            format!("route '{prefix}' statics dir must be UTF-8")
        })?;
        if text.is_empty()
            || text.contains(['\\', '%', '?', '#'])
            || text.chars().any(|character| {
                character.is_whitespace() || character.is_control()
            })
            || text.split('/').any(|part| matches!(part, "" | "." | ".."))
            || !directory
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
        {
            return Err(format!(
                "route '{prefix}' statics dir must be a plain relative directory"
            ));
        }
        let path = format!("{}/{text}", prefix.as_str().trim_end_matches('/'));
        let path: PathPrefix = serde_yaml::from_value(Value::String(path))
            .map_err(|error| {
                format!("route '{prefix}' statics dir: {error}")
            })?;
        if prefixes.contains(&path) {
            return Err(format!("duplicate statics directory at '{path}'"));
        }
        declaration
            .insert(key("path"), Value::String(path.as_str().to_owned()));
        declaration.insert(
            key("dir"),
            serde_yaml::to_value(mapping.directory().join(directory))
                .map_err(|error| error.to_string())?,
        );
        prefixes.push(path);
        compiled.push(Value::Mapping(declaration));
    }
    Ok(compiled)
}
