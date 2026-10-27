use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::config::{CacheStoreConfig, FileCacheStoreConfig};
use crate::{CacheDriver, CacheStore, Error};

#[derive(Debug, Default)]
pub struct FileCacheDriver;

#[derive(Debug, Clone)]
pub struct FileCacheStore {
    store_name: String,
    path: PathBuf,
    prefix: Option<String>,
}

impl FileCacheStore {
    pub fn new(
        store_name: impl Into<String>,
        path: PathBuf,
        prefix: Option<String>,
    ) -> Self {
        Self { store_name: store_name.into(), path, prefix }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    fn scoped_key(
        &self,
        key: &str,
    ) -> String {
        match self.prefix() {
            Some(prefix) => format!("{prefix}{key}"),
            None => key.to_string(),
        }
    }
}

#[async_trait]
impl CacheStore for FileCacheStore {
    fn store_name(&self) -> &str {
        &self.store_name
    }

    fn driver_name(&self) -> &str {
        "file"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn supports_ttl(&self) -> bool {
        false
    }

    async fn get(
        &self,
        key: &str,
    ) -> crate::Result<Option<Bytes>> {
        match cacache::read(self.path(), self.scoped_key(key)).await {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(cacache::Error::EntryNotFound(_, _)) => Ok(None),
            Err(err) => Err(Error::wrap(
                format!(
                    "file cache get failed for store '{}'",
                    self.store_name
                ),
                err,
            )),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> crate::Result<()> {
        if ttl.is_some() {
            return Err(Error::Rejected(format!(
                "file cache store '{}' does not support TTL for key '{key}'",
                self.store_name
            )));
        }

        cacache::write(self.path(), self.scoped_key(key), value)
            .await
            .map_err(|err| {
                Error::wrap(
                    format!(
                        "file cache put failed for store '{}'",
                        self.store_name
                    ),
                    err,
                )
            })?;
        Ok(())
    }

    async fn forget(
        &self,
        key: &str,
    ) -> crate::Result<()> {
        match cacache::remove(self.path(), self.scoped_key(key)).await {
            Ok(()) | Err(cacache::Error::EntryNotFound(_, _)) => Ok(()),
            Err(err) => Err(Error::wrap(
                format!(
                    "file cache forget failed for store '{}'",
                    self.store_name
                ),
                err,
            )),
        }
    }

    async fn clear(&self) -> crate::Result<()> {
        let Some(prefix) =
            self.prefix.clone().filter(|prefix| !prefix.is_empty())
        else {
            if !self.path.try_exists()? {
                return Ok(());
            }
            return cacache::clear(self.path()).await.map_err(|err| {
                Error::wrap(
                    format!(
                        "file cache clear failed for store '{}'",
                        self.store_name
                    ),
                    err,
                )
            });
        };

        let path = self.path.clone();
        let keys = tokio::task::spawn_blocking(
            move || -> crate::Result<Vec<String>> {
                if !path.try_exists()? {
                    return Ok(Vec::new());
                }
                cacache::list_sync(path)
                    .filter_map(|entry| match entry {
                        Ok(metadata) if metadata.key.starts_with(&prefix) => {
                            Some(Ok(metadata.key))
                        }
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    })
                    .collect::<cacache::Result<Vec<_>>>()
                    .map_err(Error::from)
            },
        )
        .await
        .map_err(|err| {
            Error::wrap(
                format!(
                    "file cache clear scan failed for store '{}'",
                    self.store_name
                ),
                err,
            )
        })?
        .map_err(|err| {
            Error::wrap(
                format!(
                    "file cache clear scan failed for store '{}'",
                    self.store_name
                ),
                err,
            )
        })?;

        for key in keys {
            match cacache::remove(self.path(), key).await {
                Ok(()) | Err(cacache::Error::EntryNotFound(_, _)) => {}
                Err(err) => {
                    return Err(Error::wrap(
                        format!(
                            "file cache clear failed for store '{}'",
                            self.store_name
                        ),
                        err,
                    ));
                }
            }
        }
        Ok(())
    }
}

impl CacheDriver for FileCacheDriver {
    fn driver_name(&self) -> &'static str {
        "file"
    }

    fn create_store(
        self: Arc<Self>,
        store_name: &str,
        store: &CacheStoreConfig,
    ) -> crate::Result<Arc<dyn CacheStore>> {
        let config: FileCacheStoreConfig = store.deserialize_options()?;
        Ok(Arc::new(FileCacheStore::new(
            store_name,
            PathBuf::from(config.path),
            config.prefix,
        )))
    }
}

#[cfg(test)]
#[path = "../../tests/drivers/file.rs"]
mod tests;
