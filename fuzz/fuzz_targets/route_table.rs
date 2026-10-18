#![no_main]

use claviron::http::{config::routes::RouteTable, ext::PathPrefix};
use libfuzzer_sys::fuzz_target;
use serde::{
    Deserialize,
    de::value::{Error, StrDeserializer},
};

// First line: lookup path. Remaining lines: raw route prefixes.
fuzz_target!(|data: &str| {
    let mut lines = data.split('\n');
    let path = lines.next().unwrap_or_default();
    let entries: Vec<_> = lines
        .enumerate()
        .filter_map(|(index, raw)| {
            let prefix =
                PathPrefix::deserialize(StrDeserializer::<Error>::new(raw))
                    .ok()?;
            let normalized = prefix.as_str();
            assert!(normalized.starts_with('/'));
            assert!(!normalized.chars().any(char::is_whitespace));
            assert!(normalized == "/" || !normalized.ends_with('/'));
            let roundtrip = PathPrefix::deserialize(
                StrDeserializer::<Error>::new(normalized),
            )
            .unwrap();
            assert_eq!(prefix, roundtrip);
            Some((prefix, index))
        })
        .collect();

    // Independent segment-based oracle; ties preserve declaration order.
    let expected = entries
        .iter()
        .filter(|(prefix, _)| {
            if !path.starts_with('/') {
                return false;
            }
            if prefix.as_str() == "/" {
                return true;
            }
            let mut segments = path[1..].split('/');
            prefix.as_str()[1..]
                .split('/')
                .all(|segment| segments.next() == Some(segment))
        })
        .fold(None, |best: Option<&(PathPrefix, usize)>, entry| match best {
            Some(previous)
                if previous.0.as_str().len() >= entry.0.as_str().len() =>
            {
                best
            }
            _ => Some(entry),
        })
        .map(|(_, index)| *index);
    let table = RouteTable::new(entries.clone());
    assert_eq!(table.find(path).copied(), expected);
    for (prefix, _) in &entries {
        let extended =
            format!("{}/child", prefix.as_str().trim_end_matches('/'));
        assert!(prefix.has_path_segment(&extended));
        // A lookup always returns one of the declared values.
        assert!(table.find(&extended).is_some_and(|found| {
            entries.iter().any(|(_, value)| value == found)
        }));
    }
});
