use std::borrow::Cow;
use std::collections::HashMap;

use sqlx::FromRow;
#[cfg(feature = "postgres")]
use sqlx::PgPool;
use time::{OffsetDateTime, UtcOffset};

use crate::{
    Certificate, CertificateAlias, CertificateIssuer, CertificateKind,
    HostedCertificate, HostedVhost, HostingChanged, HostingError, IssuerKind,
    NewCertificate, PublicCertificate, Result, Server, Vhost, VhostAlias,
    VhostImport,
};

/// Cloneable handle to Claviron's hosting data, in PostgreSQL (roma's
/// `hosting` schema) or, with the `sqlite` feature, in an SQLite file with
/// the same tables named `hosting_<table>`.
///
/// Only construction mentions SQLx. Operations return plain hosting domain
/// values and a storage-neutral error so consumers remain insulated from the
/// query library, the backend and table mechanics.
#[derive(Clone)]
pub struct HostingStore {
    pool: Pool,
    /// Told what a committed write changed, where the database does not say
    /// it itself (SQLite has no NOTIFY). `HostingService::store` sets it.
    announce: Option<Announce>,
}

/// Where a store says what it changed: the fleet's event bus.
pub(crate) type Announce = std::sync::Arc<dyn Fn(HostingChanged) + Send + Sync>;

#[derive(Clone)]
enum Pool {
    #[cfg(feature = "postgres")]
    Postgres(PgPool),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
}

/// One statement on whichever backend the store holds. The SQL is written
/// for PostgreSQL; [`sqlite`] rewrites the few words SQLite spells
/// differently. Statements whose shape differs are written twice instead.
macro_rules! run {
    // `execute` answers the rows it touched: the two backends' result types
    // differ, the count is what every caller reads.
    ($store:expr, query, $sql:expr, [$($bind:expr),* $(,)?], execute) => {
        match &$store.pool {
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => sqlx::query(sqlx::AssertSqlSafe($sql))
                $(.bind($bind))*
                .execute(pool)
                .await
                .map(|done| done.rows_affected()),
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => sqlx::query(sqlx::AssertSqlSafe(sqlite($sql)))
                $(.bind($bind))*
                .execute(pool)
                .await
                .map(|done| done.rows_affected())
        }
    };
    ($store:expr, $query:ident $(::<$($ty:ty),*>)?, $sql:expr, [$($bind:expr),* $(,)?], $fetch:ident) => {
        match &$store.pool {
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => {
                sqlx::$query $(::<_, $($ty),*>)?(sqlx::AssertSqlSafe($sql))
                    $(.bind($bind))*
                    .$fetch(pool)
                    .await
            }
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => {
                sqlx::$query $(::<_, $($ty),*>)?(sqlx::AssertSqlSafe(sqlite($sql)))
                    $(.bind($bind))*
                    .$fetch(pool)
                    .await
            }
        }
    };
}

/// An open transaction on either backend.
enum Tx {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
}

impl Tx {
    async fn commit(self) -> std::result::Result<(), sqlx::Error> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(tx) => tx.commit().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(tx) => tx.commit().await,
        }
    }
}

/// [`run!`] inside a transaction.
macro_rules! run_tx {
    ($tx:expr, query, $sql:expr, [$($bind:expr),* $(,)?], execute) => {
        match &mut $tx {
            #[cfg(feature = "postgres")]
            Tx::Postgres(tx) => sqlx::query(sqlx::AssertSqlSafe($sql))
                $(.bind($bind))*
                .execute(&mut **tx)
                .await
                .map(|done| done.rows_affected()),
            #[cfg(feature = "sqlite")]
            Tx::Sqlite(tx) => sqlx::query(sqlx::AssertSqlSafe(sqlite($sql)))
                $(.bind($bind))*
                .execute(&mut **tx)
                .await
                .map(|done| done.rows_affected())
        }
    };
    ($tx:expr, $query:ident $(::<$($ty:ty),*>)?, $sql:expr, [$($bind:expr),* $(,)?], $fetch:ident) => {
        match &mut $tx {
            #[cfg(feature = "postgres")]
            Tx::Postgres(tx) => {
                sqlx::$query $(::<_, $($ty),*>)?(sqlx::AssertSqlSafe($sql))
                    $(.bind($bind))*
                    .$fetch(&mut **tx)
                    .await
            }
            #[cfg(feature = "sqlite")]
            Tx::Sqlite(tx) => {
                sqlx::$query $(::<_, $($ty),*>)?(sqlx::AssertSqlSafe(sqlite($sql)))
                    $(.bind($bind))*
                    .$fetch(&mut **tx)
                    .await
            }
        }
    };
}

/// PostgreSQL's spelling of a hosting statement, as SQLite reads it:
/// `hosting.<table>` is `hosting_<table>` (SQLite has no schemas), `NOW()` is
/// UTC text of one width, and casts that only type a parameter go.
#[cfg_attr(not(feature = "sqlite"), allow(dead_code))]
fn sqlite(sql: &'static str) -> Cow<'static, str> {
    Cow::Owned(
        sql.replace("::hosting.certificate_kind", "")
            .replace("hosting.", "hosting_")
            .replace("NOW()", "strftime('%Y-%m-%dT%H:%M:%SZ', 'now')")
            .replace("::TEXT", "")
            .replace("::TIMESTAMPTZ", ""),
    )
}

/// A timestamp as both backends store it: UTC, whole seconds. SQLite keeps
/// timestamps as text and compares them as text, which is only right when
/// every one has the same width and offset.
fn stored(at: OffsetDateTime) -> OffsetDateTime {
    let at = at.to_offset(UtcOffset::UTC);
    at.replace_nanosecond(0).unwrap_or(at)
}

impl HostingStore {
    /// Announce committed writes through `announce`.
    #[cfg(feature = "server")]
    pub(crate) fn announcing(
        mut self,
        announce: Announce,
    ) -> Self {
        self.announce = Some(announce);
        self
    }

    /// PostgreSQL says what changed over NOTIFY on its own.
    #[cfg(feature = "server")]
    pub(crate) fn is_postgres(&self) -> bool {
        match &self.pool {
            #[cfg(feature = "postgres")]
            Pool::Postgres(_) => true,
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(_) => false,
        }
    }

    fn changed(
        &self,
        what: HostingChanged,
    ) {
        if let Some(announce) = &self.announce {
            announce(what);
        }
    }

    async fn begin(&self) -> Result<Tx> {
        Ok(match &self.pool {
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => Tx::Postgres(pool.begin().await?),
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => Tx::Sqlite(pool.begin().await?),
        })
    }

    /// The store on roma's PostgreSQL `hosting` schema.
    #[cfg(feature = "postgres")]
    pub fn new(pool: PgPool) -> Self {
        Self { pool: Pool::Postgres(pool), announce: None }
    }

    /// The store on an SQLite file holding the `hosting_*` tables.
    #[cfg(feature = "sqlite")]
    pub fn sqlite(pool: sqlx::SqlitePool) -> Self {
        Self { pool: Pool::Sqlite(pool), announce: None }
    }

    /// The store on the connection `db.yaml`'s `hosting:` names —
    /// `postgres.default` unless it says otherwise, e.g. `sqlite.hosting`.
    #[cfg(feature = "server")]
    pub fn open(db: &claviron_db::DbService) -> Result<Self> {
        let profile = db.hosting_profile();
        match db.profile(&profile).map_err(HostingError::Storage)? {
            #[cfg(feature = "postgres")]
            claviron_db::Pool::Postgres(pool) => Ok(Self::new(pool)),
            #[cfg(feature = "sqlite")]
            claviron_db::Pool::Sqlite(pool) => Ok(Self::sqlite(pool)),
            #[allow(unreachable_patterns)]
            _ => Err(HostingError::Storage(format!(
                "hosting: `{profile}` is on a backend hosting data is not kept in"
            ))),
        }
    }

    pub async fn find_or_create_server(
        &self,
        server_name: &str,
    ) -> Result<Server> {
        let name = server_name.trim();
        if name.is_empty() {
            return Err(HostingError::InvalidInput(
                "server name is empty".to_owned(),
            ));
        }
        if let Some(server) = self.server_by_name(name).await? {
            return Ok(server);
        }

        let inserted = run!(
            self,
            query_as::<ServerRow>,
            "INSERT INTO hosting.servers (name, created_at, enabled)
             VALUES ($1, NOW(), TRUE)
             ON CONFLICT (name) DO NOTHING
             RETURNING id, name, created_at, enabled",
            [name],
            fetch_optional
        )?;

        let server = match inserted {
            Some(server) => {
                let server: Server = server.into();
                tracing::info!(
                    server_id = server.id,
                    name = %server.name,
                    "hosting server registered"
                );
                server
            }
            None => self.server_by_name(name).await?.ok_or_else(|| {
                HostingError::Storage(format!(
                    "server '{name}' disappeared after a concurrent insert"
                ))
            })?,
        };
        Ok(server)
    }

    pub async fn server_by_name(
        &self,
        name: &str,
    ) -> Result<Option<Server>> {
        let row = run!(
            self,
            query_as::<ServerRow>,
            "SELECT id, name, created_at, enabled
               FROM hosting.servers
              WHERE name = $1",
            [name],
            fetch_optional
        )?;
        Ok(row.map(Into::into))
    }

    pub async fn add_alias_to_vhost(
        &self,
        vhost_id: i32,
        hostname: &str,
    ) -> Result<VhostAlias> {
        let hostname = hostname.trim();
        if hostname.is_empty() {
            return Err(HostingError::InvalidInput(
                "alias hostname is empty".to_owned(),
            ));
        }

        let inserted = run!(
            self,
            query_as::<VhostAliasRow>,
            "INSERT INTO hosting.vhost_aliases (vhost_id, hostname, created_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (hostname) DO NOTHING
             RETURNING id, vhost_id, hostname, created_at",
            [vhost_id, hostname],
            fetch_optional
        )?;

        if let Some(alias) = inserted {
            self.changed(HostingChanged::Vhosts);
            return Ok(alias.into());
        }

        let existing =
            self.alias_by_hostname(hostname).await?.ok_or_else(|| {
                HostingError::Storage(format!(
                    "alias '{hostname}' disappeared after a concurrent insert"
                ))
            })?;
        if existing.vhost_id == vhost_id {
            Ok(existing)
        } else {
            Err(HostingError::Conflict(format!(
                "alias '{hostname}' already belongs to vhost {}",
                existing.vhost_id
            )))
        }
    }

    /// Persist an alias for a canonical host already validated by the caller.
    /// Creates its persistence identity and server link in the same transaction.
    /// An existing alias on another host is a conflict, never an implicit move.
    pub async fn add_alias_for_server(
        &self,
        server_id: i32,
        vhost_name: &str,
        hostname: &str,
    ) -> Result<VhostAlias> {
        self.assign_alias_for_server(server_id, vhost_name, hostname, false)
            .await
    }

    /// Move a persisted alias to a caller-validated canonical host, creating
    /// the target's persistence identity and server link if needed.
    pub async fn replace_alias_for_server(
        &self,
        server_id: i32,
        vhost_name: &str,
        hostname: &str,
    ) -> Result<VhostAlias> {
        self.assign_alias_for_server(server_id, vhost_name, hostname, true)
            .await
    }

    async fn assign_alias_for_server(
        &self,
        server_id: i32,
        vhost_name: &str,
        hostname: &str,
        replace: bool,
    ) -> Result<VhostAlias> {
        let name = vhost_name.trim();
        let hostname = hostname.trim();
        if name.is_empty() || hostname.is_empty() {
            return Err(HostingError::InvalidInput(
                "vhost name and alias hostname must not be empty".to_owned(),
            ));
        }
        let mut tx = self.begin().await?;
        let inserted = run_tx!(
            tx,
            query_scalar::<i32>,
            "INSERT INTO hosting.vhosts (name, created_at, updated_at, enabled)
             VALUES ($1, NOW(), NOW(), TRUE)
             ON CONFLICT (name) DO NOTHING RETURNING id",
            [name],
            fetch_optional
        )?;
        let vhost_id = match inserted {
            Some(id) => id,
            None => run_tx!(
                tx,
                query_scalar::<i32>,
                "SELECT id FROM hosting.vhosts WHERE name = $1",
                [name],
                fetch_one
            )?,
        };
        run_tx!(
            tx,
            query,
            "INSERT INTO hosting.vhost_server (vhost_id, server_id, created_at)
             VALUES ($1, $2, NOW()) ON CONFLICT DO NOTHING",
            [vhost_id, server_id],
            execute
        )?;
        let row = if replace {
            run_tx!(
                tx,
                query_as::<VhostAliasRow>,
                "UPDATE hosting.vhost_aliases SET vhost_id = $1
                 WHERE hostname = $2
                 RETURNING id, vhost_id, hostname, created_at",
                [vhost_id, hostname],
                fetch_optional
            )?
            .ok_or_else(|| {
                HostingError::NotFound(format!("alias '{hostname}'"))
            })?
        } else {
            let inserted = run_tx!(
                tx,
                query_as::<VhostAliasRow>,
                "INSERT INTO hosting.vhost_aliases (vhost_id, hostname, created_at)
                 VALUES ($1, $2, NOW()) ON CONFLICT (hostname) DO NOTHING
                 RETURNING id, vhost_id, hostname, created_at",
                [vhost_id, hostname],
                fetch_optional
            )?;
            match inserted {
                Some(row) => row,
                None => {
                    let row = run_tx!(
                        tx,
                        query_as::<VhostAliasRow>,
                        "SELECT id, vhost_id, hostname, created_at
                         FROM hosting.vhost_aliases WHERE hostname = $1",
                        [hostname],
                        fetch_one
                    )?;
                    if row.vhost_id != vhost_id {
                        return Err(HostingError::Conflict(format!(
                            "alias '{hostname}' already belongs to vhost {}",
                            row.vhost_id
                        )));
                    }
                    row
                }
            }
        };
        tx.commit().await?;
        self.changed(HostingChanged::Vhosts);
        Ok(row.into())
    }

    pub async fn remove_alias(
        &self,
        hostname: &str,
    ) -> Result<bool> {
        let result = run!(
            self,
            query,
            "DELETE FROM hosting.vhost_aliases WHERE hostname = $1",
            [hostname],
            execute
        )?;
        if result > 0 {
            self.changed(HostingChanged::Vhosts);
        }
        Ok(result > 0)
    }

    pub async fn replace_alias(
        &self,
        hostname: &str,
        new_vhost_name: &str,
    ) -> Result<VhostAlias> {
        // SQLite's UPDATE … FROM cannot return the joined table's columns,
        // so it names the vhost through subqueries instead.
        let row = match &self.pool {
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => {
                sqlx::query_as::<_, VhostAliasRow>(
                    "UPDATE hosting.vhost_aliases AS a
                        SET vhost_id = v.id
                       FROM hosting.vhosts AS v
                      WHERE a.hostname = $1
                        AND v.name = $2
                  RETURNING a.id, a.vhost_id, a.hostname, a.created_at"
                )
                .bind(hostname)
                .bind(new_vhost_name)
                .fetch_optional(pool)
                .await?
            }
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => {
                sqlx::query_as::<_, VhostAliasRow>(
                    "UPDATE hosting_vhost_aliases
                        SET vhost_id = (SELECT id FROM hosting_vhosts WHERE name = $2)
                      WHERE hostname = $1
                        AND EXISTS (SELECT 1 FROM hosting_vhosts WHERE name = $2)
                  RETURNING id, vhost_id, hostname, created_at"
                )
                .bind(hostname)
                .bind(new_vhost_name)
                .fetch_optional(pool)
                .await?
            }
        };
        if row.is_some() {
            self.changed(HostingChanged::Vhosts);
        }
        row.map(Into::into).ok_or_else(|| {
            HostingError::NotFound(format!(
                "alias '{hostname}' or vhost '{new_vhost_name}'"
            ))
        })
    }

    pub async fn aliases_for_server(
        &self,
        server_id: i32,
    ) -> Result<HashMap<String, Vec<String>>> {
        #[derive(FromRow)]
        struct Row {
            vhost_name: String,
            hostname: String,
        }

        let rows = run!(
            self,
            query_as::<Row>,
            "SELECT v.name AS vhost_name, a.hostname
               FROM hosting.vhost_server AS vs
               JOIN hosting.vhosts AS v ON v.id = vs.vhost_id
               JOIN hosting.vhost_aliases AS a ON a.vhost_id = v.id
              WHERE vs.server_id = $1
              ORDER BY v.name, a.created_at",
            [server_id],
            fetch_all
        )?;

        let mut aliases = HashMap::<String, Vec<String>>::new();
        for row in rows {
            aliases.entry(row.vhost_name).or_default().push(row.hostname);
        }
        Ok(aliases)
    }

    pub async fn vhost_by_name(
        &self,
        name: &str,
    ) -> Result<Option<Vhost>> {
        let row = run!(
            self,
            query_as::<VhostRow>,
            "SELECT id, name, created_at, updated_at, enabled
               FROM hosting.vhosts
              WHERE name = $1",
            [name],
            fetch_optional
        )?;
        Ok(row.map(Into::into))
    }

    pub async fn vhosts_for_server(
        &self,
        server_id: i32,
    ) -> Result<Vec<Vhost>> {
        let rows = run!(
            self,
            query_as::<VhostRow>,
            "SELECT v.id, v.name, v.created_at, v.updated_at, v.enabled
               FROM hosting.vhosts AS v
               JOIN hosting.vhost_server AS vs ON vs.vhost_id = v.id
              WHERE vs.server_id = $1
              ORDER BY v.name",
            [server_id],
            fetch_all
        )?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn vhosts_with_aliases(
        &self,
        vhost_name: Option<&str>,
        enabled_only: bool,
    ) -> Result<Vec<HostedVhost>> {
        #[derive(FromRow)]
        struct Row {
            vhost_id: i32,
            vhost_name: String,
            vhost_created_at: OffsetDateTime,
            vhost_updated_at: OffsetDateTime,
            vhost_enabled: bool,
            alias_id: Option<i32>,
            alias_hostname: Option<String>,
            alias_created_at: Option<OffsetDateTime>,
        }

        let rows = run!(
            self,
            query_as::<Row>,
            "SELECT v.id AS vhost_id, v.name AS vhost_name,
                    v.created_at AS vhost_created_at,
                    v.updated_at AS vhost_updated_at,
                    v.enabled AS vhost_enabled,
                    a.id AS alias_id, a.hostname AS alias_hostname,
                    a.created_at AS alias_created_at
               FROM hosting.vhosts AS v
          LEFT JOIN hosting.vhost_aliases AS a ON a.vhost_id = v.id
              WHERE ($1::TEXT IS NULL OR v.name = $1)
                AND (NOT $2 OR v.enabled = TRUE)
              ORDER BY v.name, a.created_at",
            [vhost_name, enabled_only],
            fetch_all
        )?;

        let mut positions = HashMap::<i32, usize>::new();
        let mut result = Vec::<HostedVhost>::new();
        for row in rows {
            let position =
                *positions.entry(row.vhost_id).or_insert_with(|| {
                    let position = result.len();
                    result.push(HostedVhost {
                        vhost: Vhost {
                            id: row.vhost_id,
                            name: row.vhost_name,
                            created_at: row.vhost_created_at,
                            updated_at: row.vhost_updated_at,
                            enabled: row.vhost_enabled,
                        },
                        aliases: Vec::new(),
                    });
                    position
                });
            if let (Some(id), Some(hostname), Some(created_at)) =
                (row.alias_id, row.alias_hostname, row.alias_created_at)
            {
                result[position].aliases.push(VhostAlias {
                    id,
                    vhost_id: row.vhost_id,
                    hostname,
                    created_at,
                });
            }
        }
        Ok(result)
    }

    pub async fn import_vhosts(
        &self,
        server_id: i32,
        entries: &[VhostImport],
    ) -> Result<usize> {
        let mut tx = self.begin().await?;
        let mut inserted = 0;

        for entry in entries {
            let name = entry.name.trim();
            if name.is_empty() {
                return Err(HostingError::InvalidInput(
                    "vhost name is empty".to_owned(),
                ));
            }

            let new_id = run_tx!(
                tx,
                query_scalar::<i32>,
                "INSERT INTO hosting.vhosts (name, created_at, updated_at, enabled)
                 VALUES ($1, NOW(), NOW(), TRUE)
                 ON CONFLICT (name) DO NOTHING
                 RETURNING id",
                [name],
                fetch_optional
            )?;

            let vhost_id = match new_id {
                Some(id) => {
                    inserted += 1;
                    id
                }
                None => run_tx!(
                    tx,
                    query_scalar::<i32>,
                    "SELECT id FROM hosting.vhosts WHERE name = $1",
                    [name],
                    fetch_one
                )?,
            };

            run_tx!(
                tx,
                query,
                "INSERT INTO hosting.vhost_server (vhost_id, server_id, created_at)
                 VALUES ($1, $2, NOW())
                 ON CONFLICT DO NOTHING",
                [vhost_id, server_id],
                execute
            )?;

            for alias in &entry.aliases {
                run_tx!(
                    tx,
                    query,
                    "INSERT INTO hosting.vhost_aliases (vhost_id, hostname, created_at)
                     VALUES ($1, $2, NOW())
                     ON CONFLICT (hostname) DO NOTHING",
                    [vhost_id, alias],
                    execute
                )?;
            }
        }

        tx.commit().await?;
        self.changed(HostingChanged::Vhosts);
        Ok(inserted)
    }

    pub async fn add_vhost_to_servers(
        &self,
        vhost_name: &str,
        server_names: &[String],
    ) -> Result<usize> {
        let vhost = self.vhost_by_name(vhost_name).await?.ok_or_else(|| {
            HostingError::NotFound(format!("vhost '{vhost_name}'"))
        })?;
        let mut added = 0;
        for name in server_names {
            let server = self.find_or_create_server(name).await?;
            let result = run!(
                self,
                query,
                "INSERT INTO hosting.vhost_server (vhost_id, server_id, created_at)
                 VALUES ($1, $2, NOW())
                 ON CONFLICT DO NOTHING",
                [vhost.id, server.id],
                execute
            )?;
            if result > 0 {
                added += 1;
            }
        }
        if added > 0 {
            self.changed(HostingChanged::Vhosts);
        }
        Ok(added)
    }

    pub async fn remove_vhost_from_servers(
        &self,
        vhost_name: &str,
        server_names: &[String],
    ) -> Result<usize> {
        // SQLite has neither DELETE … USING nor arrays: the names travel as a
        // JSON list and are matched through json_each.
        let result = match &self.pool {
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => sqlx::query(
                "DELETE FROM hosting.vhost_server AS vs
                      USING hosting.vhosts AS v, hosting.servers AS s
                      WHERE vs.vhost_id = v.id
                        AND vs.server_id = s.id
                        AND v.name = $1
                        AND s.name = ANY($2)"
            )
            .bind(vhost_name)
            .bind(server_names)
            .execute(pool)
            .await?
            .rows_affected(),
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => sqlx::query(
                "DELETE FROM hosting_vhost_server
                      WHERE vhost_id = (SELECT id FROM hosting_vhosts WHERE name = $1)
                        AND server_id IN (SELECT s.id FROM hosting_servers AS s, json_each($2) AS j
                                           WHERE s.name = j.value)"
            )
            .bind(vhost_name)
            .bind(json_list(server_names))
            .execute(pool)
            .await?
            .rows_affected()
        };
        if result > 0 {
            self.changed(HostingChanged::Vhosts);
        }
        Ok(result as usize)
    }

    pub async fn servers_for_vhost(
        &self,
        vhost_name: &str,
    ) -> Result<Vec<Server>> {
        let rows = run!(
            self,
            query_as::<ServerRow>,
            "SELECT s.id, s.name, s.created_at, s.enabled
               FROM hosting.servers AS s
               JOIN hosting.vhost_server AS vs ON vs.server_id = s.id
               JOIN hosting.vhosts AS v ON v.id = vs.vhost_id
              WHERE v.name = $1
              ORDER BY s.name",
            [vhost_name],
            fetch_all
        )?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn issuer_by_id(
        &self,
        id: i32,
    ) -> Result<Option<CertificateIssuer>> {
        Ok(run!(self, query_as::<IssuerRow>,
            "SELECT id, name, kind::TEXT AS kind, acme_account_id, ca_certificate_id FROM hosting.certificate_issuers WHERE id = $1",
            [id], fetch_optional)?.map(Into::into))
    }

    /// Public chains of enabled local issuers, without loading their private keys.
    pub async fn local_issuer_certificates(
        &self
    ) -> Result<Vec<(i32, Vec<u8>)>> {
        run!(
            self,
            query_as::<(i32, Vec<u8>)>,
            "SELECT i.id, c.certs_der FROM hosting.certificate_issuers i
             JOIN hosting.certificates c ON c.id = i.ca_certificate_id
             WHERE i.kind = 'local_ca' AND c.kind = 'ca' AND c.enabled = TRUE
             ORDER BY i.id",
            [],
            fetch_all
        )
        .map_err(Into::into)
    }

    pub async fn issuer_by_name(
        &self,
        name: &str,
    ) -> Result<Option<CertificateIssuer>> {
        Ok(run!(self, query_as::<IssuerRow>,
            "SELECT id, name, kind::TEXT AS kind, acme_account_id, ca_certificate_id FROM hosting.certificate_issuers WHERE name = $1",
            [name], fetch_optional)?.map(Into::into))
    }

    /// One named issuer per ACME account, also used for accounts added after
    /// the migration. ON CONFLICT preserves an operator-renamed issuer.
    pub async fn ensure_acme_issuer(
        &self,
        account_id: i32,
    ) -> Result<CertificateIssuer> {
        let name = format!("acme-{account_id}");
        Ok(run!(self, query_as::<IssuerRow>,
            "INSERT INTO hosting.certificate_issuers (name, kind, acme_account_id)
             VALUES ($1, 'acme', $2)
             ON CONFLICT (acme_account_id) DO UPDATE SET acme_account_id = EXCLUDED.acme_account_id
             RETURNING id, name, kind::TEXT AS kind, acme_account_id, ca_certificate_id",
            [name, account_id], fetch_one)?.into())
    }

    pub async fn certificate_by_id(
        &self,
        id: i32,
    ) -> Result<Option<Certificate>> {
        Ok(run!(self, query_as::<CertificateRow>,
            "SELECT id, kind::TEXT AS kind, issuer_id, cn, certs_der, key_der, expires_at, issued_at, updated_at, enabled
               FROM hosting.certificates WHERE id = $1", [id], fetch_optional)?.map(Into::into))
    }

    /// Public material only: the private-key column is not read or decoded.
    pub async fn public_certificate_by_id(
        &self,
        id: i32,
    ) -> Result<Option<PublicCertificate>> {
        Ok(run!(self, query_as::<PublicCertificateRow>,
            "SELECT id, kind::TEXT AS kind, issuer_id, cn, certs_der, expires_at, issued_at, updated_at, enabled
               FROM hosting.certificates WHERE id = $1", [id], fetch_optional)?.map(Into::into))
    }

    /// Public material selected by its exact stored common name.
    pub async fn public_certificate_by_cn(
        &self,
        cn: &str,
    ) -> Result<Option<PublicCertificate>> {
        Ok(run!(self, query_as::<PublicCertificateRow>,
            "SELECT id, kind::TEXT AS kind, issuer_id, cn, certs_der, expires_at, issued_at, updated_at, enabled
               FROM hosting.certificates WHERE cn = $1", [cn], fetch_optional)?.map(Into::into))
    }

    /// Public records belonging to one issuer, independently of server assignment.
    /// Usage, enabled state and trust remain the consumer's policy. Private keys
    /// are never selected, including for CA records.
    pub async fn public_certificates_by_issuer(
        &self,
        issuer_id: i32,
    ) -> Result<Vec<PublicCertificate>> {
        let rows = run!(self, query_as::<PublicCertificateRow>,
            "SELECT id, kind::TEXT AS kind, issuer_id, cn, certs_der, expires_at, issued_at, updated_at, enabled
               FROM hosting.certificates WHERE issuer_id = $1 ORDER BY id",
            [issuer_id], fetch_all)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Exact usage selection before any private material is read.
    pub async fn certificate_by_cn_and_kind(
        &self,
        cn: &str,
        kind: CertificateKind,
    ) -> Result<Option<Certificate>> {
        Ok(run!(self, query_as::<CertificateRow>,
            "SELECT id, kind::TEXT AS kind, issuer_id, cn, certs_der, key_der, expires_at, issued_at, updated_at, enabled
               FROM hosting.certificates WHERE cn = $1 AND kind::TEXT = $2 AND enabled = TRUE",
            [cn, kind.as_str()], fetch_optional)?.map(Into::into))
    }

    pub async fn acme_certificates(
        &self,
        active_only: bool,
        expires_before: Option<OffsetDateTime>,
    ) -> Result<Vec<HostedCertificate>> {
        let rows = run!(self, query_as::<CertificateWithAliasRow>,
            "SELECT c.id, c.kind::TEXT AS kind, c.issuer_id, c.cn, c.certs_der, c.key_der,
                    c.expires_at, c.issued_at, c.updated_at, c.enabled,
                    a.id AS alias_id, a.hostname AS alias_hostname, a.created_at AS alias_created_at
               FROM hosting.certificates c
               JOIN hosting.certificate_issuers i ON i.id = c.issuer_id
          LEFT JOIN hosting.cert_aliases a ON a.cert_id = c.id
              WHERE i.kind = 'acme' AND c.kind = 'server'
                AND (NOT $1 OR c.enabled = TRUE)
                AND ($2::TIMESTAMPTZ IS NULL OR c.expires_at < $2)
              ORDER BY c.expires_at, c.cn, a.created_at",
            [active_only, expires_before.map(stored)], fetch_all)?;
        Ok(group_certificates(rows))
    }

    pub async fn certificates_for_server(
        &self,
        server_id: i32,
    ) -> Result<Vec<HostedCertificate>> {
        let rows = run!(
            self,
            query_as::<CertificateWithAliasRow>,
            "SELECT c.id, c.kind::TEXT AS kind, c.issuer_id, c.cn, c.certs_der, c.key_der,
                    c.expires_at, c.issued_at, c.updated_at, c.enabled,
                    a.id AS alias_id, a.hostname AS alias_hostname,
                    a.created_at AS alias_created_at
               FROM hosting.certificate_server AS cs
               JOIN hosting.certificates AS c ON c.id = cs.certificate_id
          LEFT JOIN hosting.cert_aliases AS a ON a.cert_id = c.id
              WHERE cs.server_id = $1
                AND c.enabled = TRUE AND c.kind = 'server'
              ORDER BY c.cn, a.created_at",
            [server_id],
            fetch_all
        )?;
        Ok(group_certificates(rows))
    }

    pub async fn certificate_by_cn(
        &self,
        cn: &str,
    ) -> Result<Option<HostedCertificate>> {
        let rows = run!(
            self,
            query_as::<CertificateWithAliasRow>,
            "SELECT c.id, c.kind::TEXT AS kind, c.issuer_id, c.cn, c.certs_der, c.key_der,
                    c.expires_at, c.issued_at, c.updated_at, c.enabled,
                    a.id AS alias_id, a.hostname AS alias_hostname,
                    a.created_at AS alias_created_at
               FROM hosting.certificates AS c
          LEFT JOIN hosting.cert_aliases AS a ON a.cert_id = c.id
              WHERE c.cn = $1
              ORDER BY a.created_at",
            [cn],
            fetch_all
        )?;
        Ok(group_certificates(rows).into_iter().next())
    }

    pub async fn certificate_by_hostname(
        &self,
        hostname: &str,
    ) -> Result<Option<HostedCertificate>> {
        let rows = run!(
            self,
            query_as::<CertificateWithAliasRow>,
            "SELECT c.id, c.kind::TEXT AS kind, c.issuer_id, c.cn, c.certs_der, c.key_der,
                    c.expires_at, c.issued_at, c.updated_at, c.enabled,
                    a.id AS alias_id, a.hostname AS alias_hostname,
                    a.created_at AS alias_created_at
               FROM hosting.certificates AS c
          LEFT JOIN hosting.cert_aliases AS a ON a.cert_id = c.id
              WHERE c.cn = $1 OR EXISTS (
                    SELECT 1 FROM hosting.cert_aliases AS lookup
                     WHERE lookup.cert_id = c.id AND lookup.hostname = $1
                 )
              ORDER BY c.cn, a.created_at",
            [hostname],
            fetch_all
        )?;
        let mut certificates = group_certificates(rows).into_iter();
        let first = certificates.next();
        if certificates.next().is_some() {
            return Err(HostingError::Conflict(format!(
                "hostname '{hostname}' matches multiple certificates; use the certificate CN"
            )));
        }
        Ok(first)
    }

    pub async fn certificates(
        &self,
        active_only: bool,
        expires_before: Option<OffsetDateTime>,
    ) -> Result<Vec<HostedCertificate>> {
        let rows = run!(
            self,
            query_as::<CertificateWithAliasRow>,
            "SELECT c.id, c.kind::TEXT AS kind, c.issuer_id, c.cn, c.certs_der, c.key_der,
                    c.expires_at, c.issued_at, c.updated_at, c.enabled,
                    a.id AS alias_id, a.hostname AS alias_hostname,
                    a.created_at AS alias_created_at
               FROM hosting.certificates AS c
          LEFT JOIN hosting.cert_aliases AS a ON a.cert_id = c.id
              WHERE (NOT $1 OR c.enabled = TRUE)
                AND ($2::TIMESTAMPTZ IS NULL OR c.expires_at < $2)
              ORDER BY c.expires_at, c.cn, a.created_at",
            [active_only, expires_before.map(stored)],
            fetch_all
        )?;
        Ok(group_certificates(rows))
    }

    pub async fn all_certificates_for_server(
        &self,
        server_id: i32,
    ) -> Result<Vec<HostedCertificate>> {
        let rows = run!(
            self,
            query_as::<CertificateWithAliasRow>,
            "SELECT c.id, c.kind::TEXT AS kind, c.issuer_id, c.cn, c.certs_der, c.key_der,
                    c.expires_at, c.issued_at, c.updated_at, c.enabled,
                    a.id AS alias_id, a.hostname AS alias_hostname,
                    a.created_at AS alias_created_at
               FROM hosting.certificate_server AS cs
               JOIN hosting.certificates AS c ON c.id = cs.certificate_id
          LEFT JOIN hosting.cert_aliases AS a ON a.cert_id = c.id
              WHERE cs.server_id = $1
              ORDER BY c.cn, a.created_at",
            [server_id],
            fetch_all
        )?;
        Ok(group_certificates(rows))
    }

    pub async fn insert_certificate(
        &self,
        new: NewCertificate,
        aliases: &[String],
        server_id: i32,
    ) -> Result<HostedCertificate> {
        self.persist_certificate(new, aliases, Some(server_id)).await
    }

    /// Store an identity without installing it as an inbound server certificate.
    pub async fn insert_unassigned_certificate(
        &self,
        new: NewCertificate,
        aliases: &[String],
    ) -> Result<HostedCertificate> {
        self.persist_certificate(new, aliases, None).await
    }

    async fn persist_certificate(
        &self,
        new: NewCertificate,
        aliases: &[String],
        server_id: Option<i32>,
    ) -> Result<HostedCertificate> {
        if new.kind != CertificateKind::Ca && new.issuer_id.is_none() {
            return Err(HostingError::InvalidInput(
                "leaf certificates require an issuer".into(),
            ));
        }
        if server_id.is_some() && !new.kind.is_server() {
            return Err(HostingError::InvalidInput(
                "client and CA certificates cannot be attached to servers"
                    .into(),
            ));
        }
        let mut tx = self.begin().await?;
        let row = run_tx!(
            tx,
            query_as::<CertificateRow>,
            "INSERT INTO hosting.certificates
                (issuer_id, cn, certs_der, key_der, expires_at, issued_at, updated_at, enabled, kind)
             VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8::TEXT::hosting.certificate_kind)
             RETURNING id, kind::TEXT AS kind, issuer_id, cn, certs_der, key_der, expires_at,
                       issued_at, updated_at, enabled",
            [
                new.issuer_id,
                new.cn,
                new.certs_der,
                new.key_der,
                stored(new.expires_at),
                stored(new.issued_at),
                new.enabled,
                new.kind.as_str()
            ],
            fetch_one
        ).map_err(|error| {
            if error.as_database_error().is_some_and(|db| db.is_unique_violation()) {
                HostingError::Conflict("certificate common name already exists".into())
            } else {
                HostingError::from(error)
            }
        })?;

        let mut saved_aliases = Vec::with_capacity(aliases.len());
        for hostname in aliases {
            // The resolver installs the common name independently of aliases.
            if hostname == &row.cn {
                continue;
            }
            let alias = run_tx!(
                tx,
                query_as::<CertificateAliasRow>,
                "INSERT INTO hosting.cert_aliases (cert_id, hostname, created_at)
                 VALUES ($1, $2, NOW())
                 ON CONFLICT (cert_id, hostname) DO UPDATE SET hostname = EXCLUDED.hostname
                 RETURNING id, cert_id, hostname, created_at",
                [row.id, hostname],
                fetch_one
            )?;
            saved_aliases.push(alias.into());
        }

        if row.kind == CertificateKind::Ca {
            run_tx!(
                tx,
                query,
                "INSERT INTO hosting.certificate_issuers (name, kind, ca_certificate_id) VALUES ($1, 'local_ca', $2)",
                [&row.cn, row.id],
                execute
            )?;
        }
        if let Some(server_id) = server_id {
            run_tx!(
            tx,
            query,
            "INSERT INTO hosting.certificate_server (certificate_id, server_id, created_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT DO NOTHING",
            [row.id, server_id],
            execute
        )?;
        }
        tx.commit().await?;
        self.changed(HostingChanged::Certificates);

        Ok(HostedCertificate {
            certificate: row.into(),
            aliases: saved_aliases,
        })
    }

    /// Same-key renewal; reject a concurrently replaced or disabled identity.
    /// Alias, issuer and server relations remain untouched.
    pub async fn renew_certificate_material(
        &self,
        original: &Certificate,
        certs_der: Vec<u8>,
        issued_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<HostedCertificate> {
        let updated = run!(
            self,
            query_as::<CertificateRow>,
            "UPDATE hosting.certificates
                SET certs_der = $2, issued_at = $3, expires_at = $4, updated_at = NOW()
              WHERE id = $1 AND certs_der = $5 AND key_der = $6 AND enabled = TRUE
                AND (issuer_id = $7 OR (issuer_id IS NULL AND $7 IS NULL))
                AND cn = $8 AND kind::TEXT = $9
          RETURNING id, kind::TEXT AS kind, issuer_id, cn, certs_der, key_der, expires_at,
                    issued_at, updated_at, enabled",
            [original.id, certs_der, stored(issued_at), stored(expires_at), &original.certs_der, &original.key_der, original.issuer_id, &original.cn, original.kind.as_str()],
            fetch_optional
        )?
        .ok_or_else(|| HostingError::Conflict("certificate changed or disabled during renewal".into()))?;
        self.changed(HostingChanged::Certificates);
        self.certificate_by_cn(&updated.cn).await?.ok_or_else(|| {
            HostingError::NotFound(format!("certificate '{}'", updated.cn))
        })
    }

    pub async fn update_certificate_material(
        &self,
        certificate_id: i32,
        certs_der: Vec<u8>,
        key_der: Vec<u8>,
        expires_at: OffsetDateTime,
    ) -> Result<HostedCertificate> {
        let updated = run!(
            self,
            query_as::<CertificateRow>,
            "UPDATE hosting.certificates
                SET certs_der = $2, key_der = $3, expires_at = $4, updated_at = NOW()
              WHERE id = $1
          RETURNING id, kind::TEXT AS kind, issuer_id, cn, certs_der, key_der, expires_at,
                    issued_at, updated_at, enabled",
            [certificate_id, certs_der, key_der, stored(expires_at)],
            fetch_optional
        )?
        .ok_or_else(|| HostingError::NotFound(format!("certificate id {certificate_id}")))?;
        self.changed(HostingChanged::Certificates);
        self.certificate_by_cn(&updated.cn).await?.ok_or_else(|| {
            HostingError::NotFound(format!("certificate '{}'", updated.cn))
        })
    }

    pub async fn set_certificate_enabled(
        &self,
        cn: &str,
        enabled: bool,
    ) -> Result<HostedCertificate> {
        let exists = run!(
            self,
            query_scalar::<i32>,
            "UPDATE hosting.certificates SET enabled = $2, updated_at = NOW()
              WHERE cn = $1 RETURNING id",
            [cn, enabled],
            fetch_optional
        )?;
        if exists.is_none() {
            return Err(HostingError::NotFound(format!("certificate '{cn}'")));
        }
        self.changed(HostingChanged::Certificates);
        self.certificate_by_cn(cn).await?.ok_or_else(|| {
            HostingError::NotFound(format!("certificate '{cn}'"))
        })
    }

    pub async fn delete_certificate(
        &self,
        cn: &str,
    ) -> Result<bool> {
        let result = run!(
            self,
            query,
            "DELETE FROM hosting.certificates WHERE cn = $1",
            [cn],
            execute
        )?;
        if result > 0 {
            self.changed(HostingChanged::Certificates);
        }
        Ok(result > 0)
    }

    pub async fn certificate_assigned_to_server(
        &self,
        cn: &str,
        server_id: i32,
    ) -> Result<bool> {
        let assigned = run!(
            self,
            query_scalar::<bool>,
            "SELECT EXISTS (
                SELECT 1
                  FROM hosting.certificate_server AS cs
                  JOIN hosting.certificates AS c ON c.id = cs.certificate_id
                 WHERE c.cn = $1 AND cs.server_id = $2 AND c.kind = 'server'
            )",
            [cn, server_id],
            fetch_one
        )?;
        Ok(assigned)
    }

    async fn require_server_certificate(
        &self,
        id: i32,
    ) -> Result<()> {
        let kind = run!(
            self,
            query_scalar::<String>,
            "SELECT kind::TEXT FROM hosting.certificates WHERE id = $1",
            [id],
            fetch_optional
        )?
        .ok_or_else(|| {
            HostingError::NotFound(format!("certificate id {id}"))
        })?;
        if !CertificateKind::try_from(kind)
            .map_err(|error| HostingError::InvalidInput(error.to_string()))?
            .is_server()
        {
            return Err(HostingError::InvalidInput(
                "client and CA certificates cannot be attached to servers"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Attach an existing certificate to a server.
    ///
    /// Returns `true` when a new pivot row was inserted and `false` when the
    /// certificate was already attached to that server.
    pub async fn attach_certificate_to_server(
        &self,
        certificate_id: i32,
        server_id: i32,
    ) -> Result<bool> {
        self.require_server_certificate(certificate_id).await?;
        let result = run!(
            self,
            query,
            "INSERT INTO hosting.certificate_server
                (certificate_id, server_id, created_at)
             SELECT id, $2, NOW() FROM hosting.certificates WHERE id = $1 AND kind = 'server'
             ON CONFLICT DO NOTHING",
            [certificate_id, server_id],
            execute
        )?;
        if result > 0 {
            self.changed(HostingChanged::Certificates);
        }
        Ok(result > 0)
    }

    pub async fn add_certificate_to_servers(
        &self,
        cn: &str,
        server_names: &[String],
    ) -> Result<usize> {
        let certificate_id =
            self.certificate_id_by_cn(cn).await?.ok_or_else(|| {
                HostingError::NotFound(format!("certificate '{cn}'"))
            })?;
        self.require_server_certificate(certificate_id).await?;
        let mut added = 0;
        for name in server_names {
            let server = self.find_or_create_server(name).await?;
            let result = run!(
                self,
                query,
                "INSERT INTO hosting.certificate_server
                    (certificate_id, server_id, created_at)
                 SELECT id, $2, NOW() FROM hosting.certificates WHERE id = $1 AND kind = 'server'
                 ON CONFLICT DO NOTHING",
                [certificate_id, server.id],
                execute
            )?;
            if result > 0 {
                added += 1;
            }
        }
        if added > 0 {
            self.changed(HostingChanged::Certificates);
        }
        Ok(added)
    }

    pub async fn remove_certificate_from_servers(
        &self,
        cn: &str,
        server_names: &[String],
    ) -> Result<usize> {
        let result = match &self.pool {
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => sqlx::query(
                "DELETE FROM hosting.certificate_server AS cs
                      USING hosting.certificates AS c, hosting.servers AS s
                      WHERE cs.certificate_id = c.id
                        AND cs.server_id = s.id
                        AND c.cn = $1
                        AND s.name = ANY($2)"
            )
            .bind(cn)
            .bind(server_names)
            .execute(pool)
            .await?
            .rows_affected(),
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => sqlx::query(
                "DELETE FROM hosting_certificate_server
                      WHERE certificate_id = (SELECT id FROM hosting_certificates WHERE cn = $1)
                        AND server_id IN (SELECT s.id FROM hosting_servers AS s, json_each($2) AS j
                                           WHERE s.name = j.value)"
            )
            .bind(cn)
            .bind(json_list(server_names))
            .execute(pool)
            .await?
            .rows_affected()
        };
        if result > 0 {
            self.changed(HostingChanged::Certificates);
        }
        Ok(result as usize)
    }

    pub async fn servers_for_certificate(
        &self,
        cn: &str,
    ) -> Result<Vec<Server>> {
        let rows = run!(
            self,
            query_as::<ServerRow>,
            "SELECT s.id, s.name, s.created_at, s.enabled
               FROM hosting.servers AS s
               JOIN hosting.certificate_server AS cs ON cs.server_id = s.id
               JOIN hosting.certificates AS c ON c.id = cs.certificate_id
              WHERE c.cn = $1
              ORDER BY s.name",
            [cn],
            fetch_all
        )?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn alias_by_hostname(
        &self,
        hostname: &str,
    ) -> Result<Option<VhostAlias>> {
        let row = run!(
            self,
            query_as::<VhostAliasRow>,
            "SELECT id, vhost_id, hostname, created_at
               FROM hosting.vhost_aliases
              WHERE hostname = $1",
            [hostname],
            fetch_optional
        )?;
        Ok(row.map(Into::into))
    }

    async fn certificate_id_by_cn(
        &self,
        cn: &str,
    ) -> Result<Option<i32>> {
        Ok(run!(
            self,
            query_scalar,
            "SELECT id FROM hosting.certificates WHERE cn = $1",
            [cn],
            fetch_optional
        )?)
    }
}

#[derive(FromRow)]
struct ServerRow {
    id: i32,
    name: String,
    created_at: OffsetDateTime,
    enabled: bool,
}

impl From<ServerRow> for Server {
    fn from(row: ServerRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            created_at: row.created_at,
            enabled: row.enabled,
        }
    }
}

#[derive(FromRow)]
struct VhostRow {
    id: i32,
    name: String,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    enabled: bool,
}

impl From<VhostRow> for Vhost {
    fn from(row: VhostRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            created_at: row.created_at,
            updated_at: row.updated_at,
            enabled: row.enabled,
        }
    }
}

#[derive(FromRow)]
struct VhostAliasRow {
    id: i32,
    vhost_id: i32,
    hostname: String,
    created_at: OffsetDateTime,
}

impl From<VhostAliasRow> for VhostAlias {
    fn from(row: VhostAliasRow) -> Self {
        Self {
            id: row.id,
            vhost_id: row.vhost_id,
            hostname: row.hostname,
            created_at: row.created_at,
        }
    }
}

#[derive(FromRow)]
struct CertificateWithAliasRow {
    id: i32,
    #[sqlx(try_from = "String")]
    kind: CertificateKind,
    issuer_id: Option<i32>,
    cn: String,
    certs_der: Vec<u8>,
    key_der: Vec<u8>,
    expires_at: OffsetDateTime,
    issued_at: OffsetDateTime,
    updated_at: Option<OffsetDateTime>,
    enabled: bool,
    alias_id: Option<i32>,
    alias_hostname: Option<String>,
    alias_created_at: Option<OffsetDateTime>,
}

#[derive(FromRow)]
struct PublicCertificateRow {
    id: i32,
    #[sqlx(try_from = "String")]
    kind: CertificateKind,
    issuer_id: Option<i32>,
    cn: String,
    certs_der: Vec<u8>,
    expires_at: OffsetDateTime,
    issued_at: OffsetDateTime,
    updated_at: Option<OffsetDateTime>,
    enabled: bool,
}

impl From<PublicCertificateRow> for PublicCertificate {
    fn from(row: PublicCertificateRow) -> Self {
        Self {
            id: row.id,
            kind: row.kind,
            issuer_id: row.issuer_id,
            cn: row.cn,
            certs_der: row.certs_der,
            expires_at: row.expires_at,
            issued_at: row.issued_at,
            updated_at: row.updated_at,
            enabled: row.enabled,
        }
    }
}

#[derive(FromRow)]
struct CertificateRow {
    id: i32,
    #[sqlx(try_from = "String")]
    kind: CertificateKind,
    issuer_id: Option<i32>,
    cn: String,
    certs_der: Vec<u8>,
    key_der: Vec<u8>,
    expires_at: OffsetDateTime,
    issued_at: OffsetDateTime,
    updated_at: Option<OffsetDateTime>,
    enabled: bool,
}

impl From<CertificateRow> for Certificate {
    fn from(row: CertificateRow) -> Self {
        Self {
            id: row.id,
            kind: row.kind,
            issuer_id: row.issuer_id,
            cn: row.cn,
            certs_der: row.certs_der,
            key_der: row.key_der,
            expires_at: row.expires_at,
            issued_at: row.issued_at,
            updated_at: row.updated_at,
            enabled: row.enabled,
        }
    }
}

#[derive(FromRow)]
struct CertificateAliasRow {
    id: i32,
    cert_id: i32,
    hostname: String,
    created_at: OffsetDateTime,
}

impl From<CertificateAliasRow> for CertificateAlias {
    fn from(row: CertificateAliasRow) -> Self {
        Self {
            id: row.id,
            certificate_id: row.cert_id,
            hostname: row.hostname,
            created_at: row.created_at,
        }
    }
}

fn group_certificates(
    rows: Vec<CertificateWithAliasRow>
) -> Vec<HostedCertificate> {
    let mut positions = HashMap::<i32, usize>::new();
    let mut certificates = Vec::<HostedCertificate>::new();

    for row in rows {
        let certificate_id = row.id;
        let alias =
            match (row.alias_id, row.alias_hostname, row.alias_created_at) {
                (Some(id), Some(hostname), Some(created_at)) => {
                    Some(CertificateAlias {
                        id,
                        certificate_id,
                        hostname,
                        created_at,
                    })
                }
                _ => None,
            };
        let position = *positions.entry(certificate_id).or_insert_with(|| {
            let position = certificates.len();
            certificates.push(HostedCertificate {
                certificate: Certificate {
                    id: row.id,
                    kind: row.kind,
                    issuer_id: row.issuer_id,
                    cn: row.cn,
                    certs_der: row.certs_der,
                    key_der: row.key_der,
                    expires_at: row.expires_at,
                    issued_at: row.issued_at,
                    updated_at: row.updated_at,
                    enabled: row.enabled,
                },
                aliases: Vec::new(),
            });
            position
        });
        if let Some(alias) = alias {
            certificates[position].aliases.push(alias);
        }
    }
    certificates
}

/// Names as the JSON list SQLite's json_each reads.
#[cfg(feature = "sqlite")]
fn json_list(names: &[String]) -> String {
    serde_json::to_string(names).unwrap_or_else(|_| "[]".to_owned())
}

#[cfg(test)]
#[path = "../tests/store.rs"]
mod tests;

#[derive(FromRow)]
struct IssuerRow {
    id: i32,
    name: String,
    #[sqlx(try_from = "String")]
    kind: IssuerKind,
    acme_account_id: Option<i32>,
    ca_certificate_id: Option<i32>,
}
impl From<IssuerRow> for CertificateIssuer {
    fn from(row: IssuerRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            kind: row.kind,
            acme_account_id: row.acme_account_id,
            ca_certificate_id: row.ca_certificate_id,
        }
    }
}
