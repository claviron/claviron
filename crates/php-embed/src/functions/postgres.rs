//! PHP-facing PostgreSQL primitives backed by Claviron's named pools.

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use claviron_db::DbService;
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::class::RegisteredClass;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::php_class;
use ext_php_rs::types::{ZendClassObject, ZendHashTable, Zval};
use ext_php_rs::zend::ExecuteData;
use futures_util::TryStreamExt;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgConnection, PgRow, PgTypeInfo};
use sqlx::{
    AssertSqlSafe, Column, Either, Executor, Row, SqlSafeStr, Statement,
    TypeInfo,
};
use tokio::runtime::Handle;
use tracing::debug;

use crate::extension::FunctionEntries;

mod cursor;
mod session;
mod types;

pub(crate) use cursor::PhpPostgresCursor;
pub use session::{SessionError, with_request_connection};

/// Opens the request's database scope.
///
/// From here until `end_request`, every statement this thread runs lands on one
/// pooled connection per pool, so session state behaves the way PHP expects and
/// a transaction cannot outlive the request that opened it.
pub(crate) fn begin_request() {
    if let Ok(bridge) = bridge() {
        session::begin_request(&bridge.runtime);
    }
}

/// Closes the request's database scope: an open transaction is rolled back, a
/// connection carrying session state is reset, and both go back to the pool.
///
/// Safe to call twice, which is what happens on the ordinary path — once after
/// the response, once when the next request arrives.
pub(crate) fn end_request() {
    if let Ok(bridge) = bridge() {
        session::end_request(&bridge.runtime);
    }
}

struct PhpPostgresBridge {
    service: Arc<DbService>,
    runtime: Handle,
}

const MAX_JSON_DEPTH: usize = 64;

#[php_class]
#[php(name = "Claviron\\Runtime\\PostgresConnection")]
#[php(flags = ClassFlags::from_bits_retain(
    ClassFlags::Final.bits()
        | ClassFlags::NoDynamicProperties.bits()
        | ClassFlags::NotSerializable.bits()
))]
pub(crate) struct PhpPostgresConnection {
    service: Arc<DbService>,
    name: String,
}

#[derive(Debug)]
enum Parameter {
    Null,
    Boolean(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(JsonValue),
}

/// A decoded column value on its way to PHP.
///
/// There is no JSON variant: `json`/`jsonb` reach PHP as text, the same as
/// PDO hands them over, so Eloquent's `array` and `AsArrayObject` casts decode
/// them themselves.
#[derive(Debug)]
enum Value {
    Null,
    Boolean(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    type_name: String,
}

#[derive(Debug, Default)]
struct QueryResult {
    columns: Vec<ColumnInfo>,
    rows: Vec<Vec<Value>>,
    affected_rows: u64,
}

static PHP_POSTGRES: OnceLock<ArcSwapOption<PhpPostgresBridge>> =
    OnceLock::new();

fn bridge_slot() -> &'static ArcSwapOption<PhpPostgresBridge> {
    PHP_POSTGRES.get_or_init(ArcSwapOption::const_empty)
}

pub(crate) fn configure(
    provider: Option<Arc<DbService>>
) -> claviron_runtime::registry::Result<()> {
    let bridge = provider
        .map(|service| {
            Handle::try_current()
                .map(|runtime| Arc::new(PhpPostgresBridge { service, runtime }))
                .map_err(|error| {
                    claviron_runtime::registry::Error::msg(format!(
                        "PHP PostgreSQL runtime handle is unavailable: {error}"
                    ))
                })
        })
        .transpose()?;
    let enabled = bridge.is_some();
    bridge_slot().store(bridge);
    debug!(enabled, "PHP PostgreSQL host functions configured");
    Ok(())
}

fn bridge() -> Result<Arc<PhpPostgresBridge>, String> {
    bridge_slot()
        .load_full()
        .ok_or_else(|| "Claviron PostgreSQL is not configured".to_owned())
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    let functions = [
        FunctionBuilder::new(
            "claviron_postgres_connection",
            connection_handler,
        )
        .not_required()
        .arg(Arg::new("name", DataType::String).allow_null().default("null"))
        .returns(
            DataType::object(PhpPostgresConnection::CLASS_NAME),
            false,
            false,
        )
        .build()
        .map_err(|error| format!("claviron_postgres_connection: {error}"))?,
        FunctionBuilder::new("claviron_postgres_execute", execute_handler)
            .arg(Arg::new(
                "connection",
                DataType::object(PhpPostgresConnection::CLASS_NAME),
            ))
            .arg(Arg::new("sql", DataType::String))
            .not_required()
            .arg(Arg::new("parameters", DataType::Array).default("[]"))
            .returns(DataType::Array, false, false)
            .build()
            .map_err(|error| format!("claviron_postgres_execute: {error}"))?,
        FunctionBuilder::new(
            "claviron_postgres_unprepared",
            unprepared_handler,
        )
        .arg(Arg::new(
            "connection",
            DataType::object(PhpPostgresConnection::CLASS_NAME),
        ))
        .arg(Arg::new("sql", DataType::String))
        .returns(DataType::Long, false, false)
        .build()
        .map_err(|error| format!("claviron_postgres_unprepared: {error}"))?,
        FunctionBuilder::new("claviron_postgres_begin", begin_handler)
            .arg(Arg::new(
                "connection",
                DataType::object(PhpPostgresConnection::CLASS_NAME),
            ))
            .not_required()
            .arg(
                Arg::new("isolation_level", DataType::String)
                    .allow_null()
                    .default("null"),
            )
            .arg(
                Arg::new("read_only", DataType::Bool)
                    .allow_null()
                    .default("null"),
            )
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_postgres_begin: {error}"))?,
        FunctionBuilder::new("claviron_postgres_commit", commit_handler)
            .arg(Arg::new(
                "connection",
                DataType::object(PhpPostgresConnection::CLASS_NAME),
            ))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_postgres_commit: {error}"))?,
        FunctionBuilder::new("claviron_postgres_rollback", rollback_handler)
            .arg(Arg::new(
                "connection",
                DataType::object(PhpPostgresConnection::CLASS_NAME),
            ))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_postgres_rollback: {error}"))?,
        FunctionBuilder::new(
            "claviron_postgres_in_transaction",
            in_transaction_handler,
        )
        .arg(Arg::new(
            "connection",
            DataType::object(PhpPostgresConnection::CLASS_NAME),
        ))
        .returns(DataType::Bool, false, false)
        .build()
        .map_err(|error| {
            format!("claviron_postgres_in_transaction: {error}")
        })?,
        FunctionBuilder::new(
            "claviron_postgres_disconnect",
            disconnect_handler,
        )
        .arg(Arg::new(
            "connection",
            DataType::object(PhpPostgresConnection::CLASS_NAME),
        ))
        .returns(DataType::Bool, false, false)
        .build()
        .map_err(|error| format!("claviron_postgres_disconnect: {error}"))?,
    ];
    entries.extend(functions);
    cursor::append_function_entries(entries)?;
    Ok(())
}

extern "C" fn connection_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result: Result<PhpPostgresConnection, String> = (|| {
            let mut name = Arg::new("name", DataType::String).allow_null();
            ex.parser()
                .not_required()
                .arg(&mut name)
                .parse()
                .map_err(|error| error.to_string())?;
            let name = match name.zval() {
                None => "default".to_owned(),
                Some(value) if value.is_null() => "default".to_owned(),
                Some(_) => name.consume::<String>().map_err(|_| {
                    "PostgreSQL connection name must be a string".to_owned()
                })?,
            };
            if name.is_empty() {
                return Err(
                    "PostgreSQL connection name must not be empty".to_owned()
                );
            }
            let bridge = bridge()?;
            if bridge.service.postgres_named(&name).is_none() {
                return Err(format!(
                    "PostgreSQL connection is not registered: {name}"
                ));
            }
            Ok(PhpPostgresConnection {
                service: Arc::clone(&bridge.service),
                name,
            })
        })();
        match result {
            Ok(connection) => {
                if ZendClassObject::new(connection)
                    .set_zval(retval, false)
                    .is_err()
                {
                    retval.set_null();
                    throw(
                        "failed to create Claviron\\Runtime\\PostgresConnection",
                    );
                }
            }
            Err(error) => {
                retval.set_null();
                throw(error);
            }
        }
    }));
}

extern "C" fn execute_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let (connection, sql, parameters) = parse_connection_query(ex)?;
            let bridge = bridge()?;

            session::execute(
                &connection.service,
                &bridge.runtime,
                &connection.name,
                &sql,
                parameters,
            )
        })();
        set_query_result(retval, result);
    }));
}

/// `PDO::exec()`: raw SQL, possibly several statements, no parameters; the
/// return value is the number of rows affected.
extern "C" fn unprepared_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let mut connection = Arg::new(
                "connection",
                DataType::object(PhpPostgresConnection::CLASS_NAME),
            );
            let mut sql = Arg::new("sql", DataType::String);
            ex.parser()
                .arg(&mut connection)
                .arg(&mut sql)
                .parse()
                .map_err(|error| error.to_string())?;
            let connection = connection_from_arg(&mut connection)?;
            let sql = sql
                .consume::<String>()
                .map_err(|_| "PostgreSQL SQL must be a string".to_owned())?;
            let bridge = bridge()?;

            session::execute_unprepared(
                &connection.service,
                &bridge.runtime,
                &connection.name,
                &sql,
            )
        })();
        match result {
            Ok(affected) => {
                retval.set_long(i64::try_from(affected).unwrap_or(i64::MAX))
            }
            Err(error) => {
                retval.set_null();
                throw(error);
            }
        }
    }));
}

extern "C" fn begin_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let mut connection = Arg::new(
                "connection",
                DataType::object(PhpPostgresConnection::CLASS_NAME),
            );
            let mut isolation =
                Arg::new("isolation_level", DataType::String).allow_null();
            let mut read_only =
                Arg::new("read_only", DataType::Bool).allow_null();
            ex.parser()
                .arg(&mut connection)
                .not_required()
                .arg(&mut isolation)
                .arg(&mut read_only)
                .parse()
                .map_err(|error| error.to_string())?;
            let connection = connection_from_arg(&mut connection)?;
            let statement = transaction_statement(
                optional_string(isolation)?.as_deref(),
                optional_bool(read_only)?,
            )?;
            let bridge = bridge()?;

            session::begin(
                &connection.service,
                &bridge.runtime,
                &connection.name,
                &statement,
            )
        })();
        set_bool_result(retval, result);
    }));
}

extern "C" fn commit_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        finish_handler(ex, retval, true);
    }));
}

extern "C" fn rollback_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        finish_handler(ex, retval, false);
    }));
}

fn finish_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
    commit: bool,
) {
    let result = (|| {
        let connection = parse_connection(ex)?;
        let bridge = bridge()?;

        session::finish(
            &connection.service,
            &bridge.runtime,
            &connection.name,
            commit,
        )
    })();
    set_bool_result(retval, result);
}

/// Whether the request still holds an open transaction on this connection.
///
/// PHP keeps its own nesting count, and the runtime ends any transaction the
/// request did not; this is how the two are reconciled without PHP having to
/// trust a counter that outlived the request that set it.
extern "C" fn in_transaction_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        match parse_connection(ex) {
            Ok(connection) => {
                retval.set_bool(session::in_transaction(&connection.name))
            }
            Err(error) => {
                retval.set_null();
                throw(error);
            }
        }
    }));
}

extern "C" fn disconnect_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let connection = parse_connection(ex)?;
            let bridge = bridge()?;
            session::disconnect(&bridge.runtime, &connection.name)
                .map_err(|error| error.to_string())
        })();
        set_bool_result(retval, result);
    }));
}

fn set_bool_result(
    retval: &mut Zval,
    result: Result<(), String>,
) {
    match result {
        Ok(()) => retval.set_bool(true),
        Err(error) => {
            retval.set_bool(false);
            throw(error);
        }
    }
}

/// Runs one statement, binding each parameter against the type PostgreSQL
/// reports for that placeholder.
///
/// PDO leaves parameter types to the server; sqlx sends typed binary
/// parameters. Describing the statement first gives us the server's own answer
/// and keeps both properties: no guessing, and no `text` parameter landing in a
/// `timestamp` column. Prepared statements are cached per connection, so the
/// describe is paid once per statement per connection.
async fn execute_on(
    connection: &mut PgConnection,
    sql: &str,
    parameters: Vec<Parameter>,
) -> Result<QueryResult, sqlx::Error> {
    let targets =
        describe_parameters(&mut *connection, sql, parameters.len()).await?;
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    for (index, parameter) in parameters.into_iter().enumerate() {
        query = types::bind(query, parameter, targets.get(index))
            .map_err(sqlx::Error::Encode)?;
    }
    #[allow(deprecated)]
    let mut stream = query.fetch_many(connection);
    let mut result = QueryResult::default();
    while let Some(item) = stream.try_next().await? {
        match item {
            Either::Left(done) => {
                result.affected_rows =
                    result.affected_rows.saturating_add(done.rows_affected());
            }
            Either::Right(row) => push_row(&mut result, row)?,
        }
    }
    Ok(result)
}

/// Runs raw SQL through the simple query protocol.
///
/// The extended protocol behind `execute_on` prepares the statement, and
/// PostgreSQL prepares exactly one command at a time. `PDO::exec()` never
/// prepares, which is why a migration can hand it a function body and the
/// trigger that uses it in one string; this is that path.
async fn execute_unprepared_on(
    connection: &mut PgConnection,
    sql: &str,
) -> Result<u64, sqlx::Error> {
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(connection)
        .await
        .map(|done| done.rows_affected())
}

/// Parameter types as PostgreSQL sees them.
///
/// A preparation error must reach the caller unchanged. PostgreSQL aborts an
/// open transaction on that error; retrying with natural PHP types would hide
/// the original SQLSTATE behind `25P02` and cannot repair the transaction.
async fn describe_parameters(
    connection: &mut PgConnection,
    sql: &str,
    count: usize,
) -> Result<Vec<PgTypeInfo>, sqlx::Error> {
    if count == 0 {
        return Ok(Vec::new());
    }
    // `Executor::prepare` is the describe path in sqlx 0.9: the prepared
    // statement carries PostgreSQL's own parameter types, and the driver keeps
    // it in the per-connection statement cache.
    let statement = Executor::prepare(
        &mut *connection,
        AssertSqlSafe(sql.to_owned()).into_sql_str(),
    )
    .await?;
    Ok(match statement.parameters() {
        Some(Either::Left(types)) => types.to_vec(),
        _ => Vec::new(),
    })
}

fn push_row(
    result: &mut QueryResult,
    row: PgRow,
) -> Result<(), sqlx::Error> {
    if result.columns.is_empty() {
        result.columns = row
            .columns()
            .iter()
            .map(|column| ColumnInfo {
                name: column.name().to_owned(),
                type_name: column.type_info().name().to_owned(),
            })
            .collect();
    }
    let values = row
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            types::decode(&row, index, column.name(), column.type_info().name())
        })
        .collect::<Result<Vec<_>, _>>()?;
    result.rows.push(values);
    Ok(())
}

fn parse_connection_query(
    ex: &mut ExecuteData
) -> Result<(PhpPostgresConnection, String, Vec<Parameter>), String> {
    let mut connection = Arg::new(
        "connection",
        DataType::object(PhpPostgresConnection::CLASS_NAME),
    );
    let mut sql = Arg::new("sql", DataType::String);
    let mut parameters = Arg::new("parameters", DataType::Array);
    ex.parser()
        .arg(&mut connection)
        .arg(&mut sql)
        .not_required()
        .arg(&mut parameters)
        .parse()
        .map_err(|error| error.to_string())?;
    Ok((
        connection_from_arg(&mut connection)?,
        sql.consume::<String>()
            .map_err(|_| "PostgreSQL SQL must be a string".to_owned())?,
        parse_parameters(parameters)?,
    ))
}

fn parse_connection(
    ex: &mut ExecuteData
) -> Result<PhpPostgresConnection, String> {
    let mut connection = Arg::new(
        "connection",
        DataType::object(PhpPostgresConnection::CLASS_NAME),
    );
    ex.parser()
        .arg(&mut connection)
        .parse()
        .map_err(|error| error.to_string())?;
    connection_from_arg(&mut connection)
}

fn connection_from_arg(
    arg: &mut Arg<'_>
) -> Result<PhpPostgresConnection, String> {
    let object =
        arg.zval().and_then(|value| value.object()).ok_or_else(|| {
            "connection must be a Claviron\\Runtime\\PostgresConnection"
                .to_owned()
        })?;
    let handle =
        ZendClassObject::<PhpPostgresConnection>::from_zend_obj(object)
            .ok_or_else(|| {
                "connection must be a Claviron\\Runtime\\PostgresConnection"
                    .to_owned()
            })?;
    Ok(PhpPostgresConnection {
        service: Arc::clone(&handle.service),
        name: handle.name.clone(),
    })
}

fn parse_parameters(mut arg: Arg<'_>) -> Result<Vec<Parameter>, String> {
    let Some(value) = arg.zval() else {
        return Ok(Vec::new());
    };
    let array = value
        .array()
        .ok_or_else(|| "PostgreSQL parameters must be an array".to_owned())?;
    array.values().map(parse_parameter).collect()
}

fn parse_parameter(value: &Zval) -> Result<Parameter, String> {
    let value = value.dereference();
    if value.is_null() {
        return Ok(Parameter::Null);
    }
    if let Some(value) = value.bool() {
        return Ok(Parameter::Boolean(value));
    }
    if let Some(value) = value.long() {
        return Ok(Parameter::I64(value));
    }
    if let Some(value) = value.double() {
        return value.is_finite().then_some(Parameter::F64(value)).ok_or_else(
            || "PostgreSQL float parameters must be finite".to_owned(),
        );
    }
    if let Some(value) = value.binary_slice::<u8>() {
        return String::from_utf8(value.to_vec())
            .map(Parameter::String)
            .map_err(|_| {
                "binary PostgreSQL parameters must use the tagged bytes form"
                    .to_owned()
            });
    }
    let array = value.array().ok_or_else(|| {
        "unsupported PostgreSQL parameter; use a scalar or tagged parameter array".to_owned()
    })?;
    parse_tagged_parameter(array)
}

fn parse_tagged_parameter(array: &ZendHashTable) -> Result<Parameter, String> {
    let kind = array
        .get("type")
        .and_then(|value| value.binary_slice::<u8>())
        .and_then(|value| std::str::from_utf8(value).ok())
        .ok_or_else(|| {
            "tagged PostgreSQL parameter requires a string 'type'".to_owned()
        })?;
    let value = array.get("value");
    match kind {
        "null" => Ok(Parameter::Null),
        "boolean" => value
            .and_then(Zval::bool)
            .map(Parameter::Boolean)
            .ok_or_else(|| {
                "boolean PostgreSQL parameter requires a bool value".to_owned()
            }),
        "i32" => value
            .and_then(Zval::long)
            .and_then(|value| i32::try_from(value).ok())
            .map(Parameter::I32)
            .ok_or_else(|| {
                "i32 PostgreSQL parameter is outside the 32-bit range"
                    .to_owned()
            }),
        "i64" => {
            value.and_then(Zval::long).map(Parameter::I64).ok_or_else(|| {
                "i64 PostgreSQL parameter requires an integer value".to_owned()
            })
        }
        "f64" => value
            .and_then(Zval::double)
            .filter(|value| value.is_finite())
            .map(Parameter::F64)
            .ok_or_else(|| {
                "f64 PostgreSQL parameter requires a finite float value"
                    .to_owned()
            }),
        "string" => value
            .and_then(Zval::string)
            .map(Parameter::String)
            .ok_or_else(|| {
                "string PostgreSQL parameter requires a UTF-8 string value"
                    .to_owned()
            }),
        "bytes" => value
            .and_then(|value| value.binary::<u8>())
            .map(Parameter::Bytes)
            .ok_or_else(|| {
                "bytes PostgreSQL parameter requires a string value".to_owned()
            }),
        "json" => value
            .ok_or_else(|| {
                "json PostgreSQL parameter requires a value".to_owned()
            })
            .and_then(json_from_zval)
            .map(Parameter::Json),
        _ => {
            Err(format!("unsupported tagged PostgreSQL parameter type: {kind}"))
        }
    }
}

fn json_from_zval(value: &Zval) -> Result<JsonValue, String> {
    json_from_zval_inner(value, 0, &mut HashSet::new())
}

fn json_from_zval_inner(
    value: &Zval,
    depth: usize,
    ancestors: &mut HashSet<usize>,
) -> Result<JsonValue, String> {
    if depth > MAX_JSON_DEPTH {
        return Err(format!(
            "JSON PostgreSQL parameter exceeds maximum nesting depth {MAX_JSON_DEPTH}"
        ));
    }
    let value = value.dereference();
    if value.is_null() {
        return Ok(JsonValue::Null);
    }
    if let Some(value) = value.bool() {
        return Ok(JsonValue::Bool(value));
    }
    if let Some(value) = value.long() {
        return Ok(JsonValue::from(value));
    }
    if let Some(value) = value.double() {
        return serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| "JSON floats must be finite".to_owned());
    }
    if let Some(value) = value.string() {
        return Ok(JsonValue::String(value));
    }
    let array = value.array().ok_or_else(|| {
        "JSON PostgreSQL parameter supports only null, bool, int, float, string, or array"
            .to_owned()
    })?;
    let identity = std::ptr::from_ref(array) as usize;
    if !ancestors.insert(identity) {
        return Err(
            "JSON PostgreSQL parameter contains a recursive array".to_owned()
        );
    }
    if array.has_sequential_keys() {
        let result = array
            .values()
            .map(|value| json_from_zval_inner(value, depth + 1, ancestors))
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array);
        ancestors.remove(&identity);
        return result;
    }
    let mut object = serde_json::Map::with_capacity(array.len());
    for (key, value) in array {
        object.insert(
            key.to_string(),
            json_from_zval_inner(value, depth + 1, ancestors)?,
        );
    }
    ancestors.remove(&identity);
    Ok(JsonValue::Object(object))
}

fn transaction_statement(
    isolation: Option<&str>,
    read_only: Option<bool>,
) -> Result<String, String> {
    let mut statement = "BEGIN".to_owned();
    if let Some(isolation) = isolation {
        statement.push_str(" ISOLATION LEVEL ");
        statement.push_str(match isolation {
            "read uncommitted" => "READ UNCOMMITTED",
            "read committed" => "READ COMMITTED",
            "repeatable read" => "REPEATABLE READ",
            "serializable" => "SERIALIZABLE",
            _ => {
                return Err(format!(
                    "unsupported PostgreSQL isolation level: {isolation}"
                ));
            }
        });
    }
    if let Some(read_only) = read_only {
        statement.push_str(if read_only {
            " READ ONLY"
        } else {
            " READ WRITE"
        });
    }
    Ok(statement)
}

fn optional_string(mut arg: Arg<'_>) -> Result<Option<String>, String> {
    match arg.zval() {
        None => Ok(None),
        Some(value) if value.is_null() => Ok(None),
        Some(_) => arg.consume::<String>().map(Some).map_err(|_| {
            "PostgreSQL isolation level must be a string".to_owned()
        }),
    }
}

fn optional_bool(mut arg: Arg<'_>) -> Result<Option<bool>, String> {
    match arg.zval() {
        None => Ok(None),
        Some(value) if value.is_null() => Ok(None),
        Some(_) => arg
            .consume::<bool>()
            .map(Some)
            .map_err(|_| "PostgreSQL read_only must be a bool".to_owned()),
    }
}

fn set_query_result(
    retval: &mut Zval,
    result: Result<QueryResult, String>,
) {
    match result {
        Ok(result) => {
            if let Err(error) = query_result_zval(result, retval) {
                retval.set_null();
                throw(error);
            }
        }
        Err(error) => {
            retval.set_null();
            throw(error);
        }
    }
}

fn query_result_zval(
    result: QueryResult,
    retval: &mut Zval,
) -> Result<(), String> {
    let mut output = ZendHashTable::new();
    output
        .insert("columns", columns_zval(result.columns)?)
        .map_err(|error| error.to_string())?;
    output
        .insert("rows", rows_zval(result.rows)?)
        .map_err(|error| error.to_string())?;
    let mut affected = Zval::new();
    if let Ok(value) = i64::try_from(result.affected_rows) {
        affected.set_long(value);
    } else {
        affected
            .set_string(&result.affected_rows.to_string(), false)
            .map_err(|error| error.to_string())?;
    }
    output
        .insert("affected_rows", affected)
        .map_err(|error| error.to_string())?;
    retval.set_hashtable(output);
    Ok(())
}

fn columns_zval(columns: Vec<ColumnInfo>) -> Result<Zval, String> {
    let mut output = ZendHashTable::with_capacity(
        columns.len().try_into().unwrap_or(u32::MAX),
    );
    for column in columns {
        let mut value = ZendHashTable::new();
        value.insert("name", column.name).map_err(|error| error.to_string())?;
        value
            .insert("type", column.type_name)
            .map_err(|error| error.to_string())?;
        output.push(value).map_err(|error| error.to_string())?;
    }
    let mut value = Zval::new();
    value.set_hashtable(output);
    Ok(value)
}

fn rows_zval(rows: Vec<Vec<Value>>) -> Result<Zval, String> {
    let mut output =
        ZendHashTable::with_capacity(rows.len().try_into().unwrap_or(u32::MAX));
    for row in rows {
        let mut values = ZendHashTable::with_capacity(
            row.len().try_into().unwrap_or(u32::MAX),
        );
        for value in row {
            values
                .push(value_zval(value)?)
                .map_err(|error| error.to_string())?;
        }
        output.push(values).map_err(|error| error.to_string())?;
    }
    let mut value = Zval::new();
    value.set_hashtable(output);
    Ok(value)
}

fn value_zval(value: Value) -> Result<Zval, String> {
    let mut output = Zval::new();
    match value {
        Value::Null => output.set_null(),
        Value::Boolean(value) => output.set_bool(value),
        Value::I32(value) => output.set_long(i64::from(value)),
        Value::I64(value) => output.set_long(value),
        Value::F64(value) => output.set_double(value),
        Value::String(value) => {
            output
                .set_string(&value, false)
                .map_err(|error| error.to_string())?;
        }
        Value::Bytes(value) => output.set_binary(value),
    }
    Ok(output)
}

/// Reports a database failure the way PDO does: `SQLSTATE[40P01]: <message>`.
///
/// The shape is the contract, not decoration. Everything downstream in a PHP
/// framework reads a database error the way PDO reports it — concurrency
/// detection deciding whether a transaction is worth retrying, lost-connection
/// detection deciding whether to reconnect, a package recognising a unique
/// violation. Flattening the error to its text threw the SQLSTATE away and left
/// all of that guessing at substrings.
pub(super) fn describe(error: &sqlx::Error) -> String {
    let sqlx::Error::Database(database) = error else {
        return error.to_string();
    };

    match database.code() {
        Some(code) => format!("SQLSTATE[{code}]: {}", database.message()),
        None => database.message().to_owned(),
    }
}

fn throw(error: impl std::fmt::Display) {
    PhpException::from_message(format!(
        "Claviron PostgreSQL operation failed: {error}"
    ))
    .throw();
}

#[cfg(test)]
#[path = "../../tests/functions/postgres.rs"]
mod tests;
