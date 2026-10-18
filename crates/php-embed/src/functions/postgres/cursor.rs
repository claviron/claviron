//! Server-side cursors: a result set that stays on PostgreSQL.
//!
//! A query run through `claviron_postgres_execute` materialises every row
//! before PHP sees any of them, which is fine for a page of results and fatal
//! for a background job walking a large table. A cursor moves that cost to the
//! server: PHP asks for a batch at a time and never holds more than one.
//!
//! The cursor is a `DECLARE`/`FETCH`/`CLOSE` on the request's own connection
//! rather than a Rust stream held between two PHP calls. A stream would have to
//! borrow the connection across the return into PHP and back, and there is no
//! honest way to hold that borrow; a cursor name is just a name, and the
//! connection it belongs to is already pinned to the caller for as long as the
//! cursor is open.
//!
//! `WITH HOLD` is chosen by whether a transaction is already open. Inside one,
//! a plain cursor streams: rows are read as they are fetched. Outside one,
//! PostgreSQL would close the cursor at the end of the implicit transaction
//! that declared it, so `WITH HOLD` is used and the server materialises the
//! remainder into its own temporary storage at that point. Both bound PHP's
//! memory, which is the point; only the first also bounds the server's. Opening
//! a transaction on the caller's behalf would be the third option and is
//! deliberately not taken — a hidden transaction around an iteration changes
//! what every other statement in the request sees.

use std::cell::Cell;
use std::sync::Arc;

use claviron_db::DbService;
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::class::RegisteredClass;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::php_class;
use ext_php_rs::types::{ZendClassObject, Zval};
use ext_php_rs::zend::ExecuteData;

use super::{
    Parameter, bridge, parse_parameters, session, set_query_result, throw,
};
use crate::extension::FunctionEntries;

/// How many rows one `FETCH` asks for when the caller does not say.
const DEFAULT_BATCH: i64 = 1_000;

#[php_class]
#[php(name = "Claviron\\Runtime\\PostgresCursor")]
#[php(flags = ClassFlags::from_bits_retain(
    ClassFlags::Final.bits()
        | ClassFlags::NoDynamicProperties.bits()
        | ClassFlags::NotSerializable.bits()
))]
pub(crate) struct PhpPostgresCursor {
    service: Arc<DbService>,
    pool: String,
    name: String,
    batch: i64,
    /// Closing twice is not an error worth raising, and a caller that abandons
    /// an iteration mid-way will close in a `finally` that may already have run.
    open: Cell<bool>,
}

pub(super) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.push(
        FunctionBuilder::new("claviron_postgres_cursor", open_handler)
            .arg(Arg::new(
                "connection",
                DataType::object(super::PhpPostgresConnection::CLASS_NAME),
            ))
            .arg(Arg::new("sql", DataType::String))
            .not_required()
            .arg(Arg::new("parameters", DataType::Array).default("[]"))
            .arg(Arg::new("batch", DataType::Long).default("1000"))
            .returns(
                DataType::object(PhpPostgresCursor::CLASS_NAME),
                false,
                false,
            )
            .build()
            .map_err(|error| format!("claviron_postgres_cursor: {error}"))?,
    );
    entries.push(
        FunctionBuilder::new("claviron_postgres_cursor_fetch", fetch_handler)
            .arg(Arg::new(
                "cursor",
                DataType::object(PhpPostgresCursor::CLASS_NAME),
            ))
            .returns(DataType::Array, false, false)
            .build()
            .map_err(|error| {
                format!("claviron_postgres_cursor_fetch: {error}")
            })?,
    );
    entries.push(
        FunctionBuilder::new("claviron_postgres_cursor_close", close_handler)
            .arg(Arg::new(
                "cursor",
                DataType::object(PhpPostgresCursor::CLASS_NAME),
            ))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| {
                format!("claviron_postgres_cursor_close: {error}")
            })?,
    );

    Ok(())
}

extern "C" fn open_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result: Result<PhpPostgresCursor, String> = (|| {
            let mut connection = Arg::new(
                "connection",
                DataType::object(super::PhpPostgresConnection::CLASS_NAME),
            );
            let mut sql = Arg::new("sql", DataType::String);
            let mut parameters = Arg::new("parameters", DataType::Array);
            let mut batch = Arg::new("batch", DataType::Long);
            ex.parser()
                .arg(&mut connection)
                .arg(&mut sql)
                .not_required()
                .arg(&mut parameters)
                .arg(&mut batch)
                .parse()
                .map_err(|error| error.to_string())?;

            let connection = super::connection_from_arg(&mut connection)?;
            let sql = sql
                .consume::<String>()
                .map_err(|_| "PostgreSQL SQL must be a string".to_owned())?;
            let parameters = parse_parameters(parameters)?;
            let batch = batch
                .zval()
                .and_then(|value| value.long())
                .unwrap_or(DEFAULT_BATCH);
            if batch < 1 {
                return Err("PostgreSQL cursor batch size must be at least 1"
                    .to_owned());
            }

            let bridge = bridge()?;
            let name = next_name();
            let hold = !session::in_transaction(&connection.name);

            session::declare_cursor(
                &connection.service,
                &bridge.runtime,
                &connection.name,
                &declare(&name, hold, &sql),
                parameters,
            )?;

            Ok(PhpPostgresCursor {
                service: Arc::clone(&connection.service),
                pool: connection.name,
                name,
                batch,
                open: Cell::new(true),
            })
        })();

        match result {
            Ok(cursor) => {
                if ZendClassObject::new(cursor).set_zval(retval, false).is_err()
                {
                    retval.set_null();
                    throw("failed to create Claviron\\Runtime\\PostgresCursor");
                }
            }
            Err(error) => {
                retval.set_null();
                throw(error);
            }
        }
    }));
}

/// Reads the next batch. An empty row list is how exhaustion is reported, which
/// is what `FETCH` itself does.
extern "C" fn fetch_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = with_cursor(ex, |cursor| {
            if !cursor.open.get() {
                return Err("PostgreSQL cursor is closed".to_owned());
            }

            let bridge = bridge()?;
            let sql = format!(
                "FETCH FORWARD {} FROM {}",
                cursor.batch,
                quote(&cursor.name)
            );

            session::execute(
                &cursor.service,
                &bridge.runtime,
                &cursor.pool,
                &sql,
                Vec::<Parameter>::new(),
            )
        });
        set_query_result(retval, result);
    }));
}

extern "C" fn close_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = with_cursor(ex, |cursor| {
            if !cursor.open.replace(false) {
                return Ok(());
            }

            let bridge = bridge()?;

            session::close_cursor(
                &cursor.service,
                &bridge.runtime,
                &cursor.pool,
                &format!("CLOSE {}", quote(&cursor.name)),
            )
        });

        match result {
            Ok(()) => retval.set_bool(true),
            Err(error) => {
                retval.set_bool(false);
                throw(error);
            }
        }
    }));
}

/// Runs `with` against the cursor argument while the borrow that produced it is
/// still alive — a reference to it cannot outlive the parsed argument.
fn with_cursor<T>(
    ex: &mut ExecuteData,
    with: impl FnOnce(&PhpPostgresCursor) -> Result<T, String>,
) -> Result<T, String> {
    let mut cursor =
        Arg::new("cursor", DataType::object(PhpPostgresCursor::CLASS_NAME));
    ex.parser().arg(&mut cursor).parse().map_err(|error| error.to_string())?;

    let object =
        cursor.zval().and_then(|value| value.object()).ok_or_else(|| {
            "cursor must be a Claviron\\Runtime\\PostgresCursor".to_owned()
        })?;
    let handle = ZendClassObject::<PhpPostgresCursor>::from_zend_obj(object)
        .ok_or_else(|| {
            "cursor must be a Claviron\\Runtime\\PostgresCursor".to_owned()
        })?;

    with(handle)
}

fn declare(
    name: &str,
    hold: bool,
    sql: &str,
) -> String {
    format!(
        "DECLARE {} NO SCROLL CURSOR {} FOR {sql}",
        quote(name),
        if hold { "WITH HOLD" } else { "WITHOUT HOLD" }
    )
}

/// Cursor names are generated here, never taken from the caller, so this only
/// has to survive the identifier rules — but it is quoted anyway, because a
/// name that reaches SQL unquoted is a habit worth not having.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn next_name() -> String {
    thread_local! {
        static NEXT: Cell<u64> = const { Cell::new(0) };
    }

    NEXT.with(|next| {
        let id = next.get().wrapping_add(1);
        next.set(id);

        format!("claviron_cursor_{id}")
    })
}
