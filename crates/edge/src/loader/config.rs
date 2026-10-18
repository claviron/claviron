//! Host maps as Congif prepares them: a consumer-owned declaration or
//! `vhost.yaml` and its `includes`, merged into one host map.
//!
//! Congif reads the root, strips the reserved `include` / `includes` keys,
//! resolves each entry beside the root (a glob to the files present at that
//! read) and watches them; it opens none of them. The merge is this loader's:
//! [`read_document`] reads every include through the owner's reader, so each
//! is parsed under the same environment, and folds it in here.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use claviron_runtime::congif::{
    self, ConfigFile, ConfigReader, ConfigValue, Configurable,
};
use claviron_types::Host;
use serde::Deserialize;
use serde::de::{Deserializer, Error as _};

use crate::config::HostConfig;
use crate::loader::table::VhostTable;

/// A host map, before alias and policy compilation. Used for `vhost.yaml`
/// and its includes, or nested in a consumer's own `Configurable` config.
/// Equality is the declared value's: `HostConfig` is runtime shape and
/// compares nothing, so the raw entries stand for it.
#[derive(Default, Clone)]
pub struct VhostDocument {
    raw: BTreeMap<String, ConfigValue>,
    hosts: BTreeMap<String, Arc<HostConfig>>,
    /// Prepared file parts, not another runtime config store. Leaf documents
    /// have no file cache of their own; the aggregate owns this source graph.
    files: BTreeMap<PathBuf, Arc<VhostDocument>>,
    includes: BTreeMap<PathBuf, Vec<PathBuf>>,
    root: Option<PathBuf>,
}

impl PartialEq for VhostDocument {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        self.raw == other.raw
            && self.includes == other.includes
            && self.files.keys().eq(other.files.keys())
            && self.root == other.root
    }
}

impl<'de> Deserialize<'de> for VhostDocument {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        let raw = BTreeMap::<String, ConfigValue>::deserialize(deserializer)?;
        let mut hosts = BTreeMap::new();
        for (host, value) in &raw {
            let config =
                HostConfig::deserialize(value.clone()).map_err(|error| {
                    D::Error::custom(format!("{host}: {error}"))
                })?;
            hosts.insert(host.clone(), Arc::new(config));
        }
        Ok(Self { raw, hosts, ..Self::default() })
    }
}

impl VhostDocument {
    /// Keep the runtime identity of declarations whose content did not change.
    pub(crate) fn retain_unchanged(
        &mut self,
        previous: &Self,
    ) {
        for (key, config) in &mut self.hosts {
            if self.raw.get(key) == previous.raw.get(key)
                && let Some(old) = previous.hosts.get(key)
            {
                *config = old.clone();
            }
        }
    }

    /// Typed declarations for `claviron::edge::register_hosts` sources.
    /// Values share the prepared configuration Arcs; nothing is re-decoded.
    pub fn declarations(&self) -> crate::Result<Vec<(Host, Arc<HostConfig>)>> {
        self.hosts
            .iter()
            .map(|(key, config)| {
                let host = key.parse().map_err(|error| {
                    crate::Error::Invalid(format!("{key}: {error}"))
                })?;
                Ok((host, Arc::clone(config)))
            })
            .collect()
    }

    /// Fold an included document in. A canonical host declared twice is a
    /// hard error: a reload fails loudly rather than publish a surprising
    /// table.
    fn merge(
        &mut self,
        part: VhostDocument,
        path: &Path,
    ) -> congif::Result<()> {
        for (host, value) in part.raw {
            if self.raw.insert(host.clone(), value).is_some() {
                return Err(congif::Error::msg(format!(
                    "duplicate vhost `{host}` while loading {}",
                    path.display()
                )));
            }
        }
        self.hosts.extend(part.hosts);
        Ok(())
    }

    /// The declared hosts as a table: canonical keys parsed, aliases seeded.
    pub(crate) fn table(&self) -> crate::Result<VhostTable> {
        self.table_with(std::iter::empty())
    }

    /// Consumer declarations share the file hosts' validation and alias
    /// namespace; neither source silently replaces the other.
    pub(crate) fn table_with(
        &self,
        hosts: impl IntoIterator<Item = (Host, Arc<HostConfig>)>,
    ) -> crate::Result<VhostTable> {
        VhostTable::from_host_map(
            self.hosts
                .iter()
                .map(|(host, config)| (host.clone(), config.clone()))
                .chain(
                    hosts
                        .into_iter()
                        .map(|(host, config)| (host.to_string(), config)),
                ),
        )
    }

    pub fn host_count(&self) -> usize {
        self.hosts.len()
    }
}

/// The owner's document with its includes merged, recursively: a file an
/// include names is read too, once, and its own includes follow. The
/// root's absence is the empty document (the owner's `absent_source`).
pub(crate) async fn read_document<O>(
    owner: &O,
    reader: &ConfigReader,
) -> congif::Result<VhostDocument>
where
    O: Configurable<Config = VhostDocument> + 'static,
{
    let source = owner.source().path;
    let root = normalized_source(&source).await?;
    let part: VhostDocument = reader.read_source(owner).await?;
    let mut document = VhostDocument::default();
    document.root = Some(root.clone());
    document.files.insert(root.clone(), Arc::new(part));
    document.includes.insert(root.clone(), reader.includes_for(&source)?);
    assemble_document(owner, reader, document, None, &[]).await
}

/// Only registered partial sources are read. Unchanged source parts retain
/// their typed host Arcs; new nested includes are read before validation.
pub(crate) async fn read_partial_document<O>(
    owner: &O,
    reader: &ConfigReader,
    changed: &[PathBuf],
) -> congif::Result<VhostDocument>
where
    O: Configurable<Config = VhostDocument> + 'static,
{
    let previous = reader.snapshot::<O>()?;
    if previous.root.is_none() {
        return read_document(owner, reader).await;
    }
    let mut document = VhostDocument::default();
    document.root = previous.root.clone();
    assemble_document(owner, reader, document, Some(&previous), changed).await
}

async fn assemble_document<O>(
    owner: &O,
    reader: &ConfigReader,
    mut document: VhostDocument,
    previous: Option<&VhostDocument>,
    changed: &[PathBuf],
) -> congif::Result<VhostDocument>
where
    O: Configurable<Config = VhostDocument> + 'static,
{
    let root = document.root.clone().expect("document root prepared");
    let mut seen = BTreeSet::new();
    let mut pending = std::collections::VecDeque::from([root.clone()]);
    while let Some(path) = pending.pop_front() {
        if !seen.insert(path.clone()) {
            continue;
        }
        let index_path =
            if path == root { owner.source().path } else { path.clone() };
        let recorded = reader.includes_for(&index_path)?;
        // Rejected preparation may already have discovered a new include.
        // Refresh its declaring part rather than hiding the correction behind
        // the last accepted source graph.
        let cached = previous
            .filter(|previous| {
                !changed.contains(&path)
                    && previous.includes.get(&path) == Some(&recorded)
            })
            .and_then(|previous| previous.files.get(&path))
            .cloned();
        let part =
            if let Some(part) = document.files.get(&path).cloned().or(cached) {
                document.includes.insert(path.clone(), recorded);
                part
            } else {
                let part: VhostDocument = if path == root {
                    reader.read_source(owner).await?
                } else {
                    reader.read_include(owner, &ConfigFile::yaml(&path)).await?
                };
                document
                    .includes
                    .insert(path.clone(), reader.includes_for(&index_path)?);
                Arc::new(part)
            };
        pending
            .extend(document.includes.get(&path).cloned().unwrap_or_default());
        document.merge(part.as_ref().clone(), &path)?;
        document.files.insert(path, part);
    }
    Ok(document)
}

/// Registered include dependencies, including nested includes discovered by
/// the domain reader. Congif records paths but does not open these sources.
pub(crate) fn partial_sources(
    owner: &impl Configurable,
    reader: &ConfigReader,
) -> congif::Result<Vec<PathBuf>> {
    let source = owner.source().path;
    let mut pending: std::collections::VecDeque<_> =
        reader.includes_for(&source)?.into();
    let mut seen = BTreeSet::new();
    while let Some(path) = pending.pop_front() {
        if seen.insert(path.clone()) {
            pending.extend(reader.includes_for(&path)?);
        }
    }
    Ok(seen.into_iter().collect())
}

async fn normalized_source(path: &Path) -> congif::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = tokio::fs::canonicalize(parent).await.map_err(|source| {
        congif::Error::Read { path: parent.to_owned(), source }
    })?;
    let name = path
        .file_name()
        .ok_or_else(|| congif::Error::msg("vhost source has no filename"))?;
    Ok(parent.join(name))
}
