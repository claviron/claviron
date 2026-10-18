//! The engine's execution budget, for a persistent program to bound one unit
//! of its own work.
//!
//! PHP's own `max_execution_time` timer is cancelled on this host
//! (`sapi::execute_php`), and a daemon runs for as long as it likes. When it
//! takes an invocation and wants that one unit bounded, it arms the same
//! budget the engine arms around a script: the fatal error PHP raises names
//! the seconds asked for, and `claviron_budget_end()` disarms it.

use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::ExecuteData;

use crate::engine::interrupt;
use crate::extension::FunctionEntries;

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.push(
        FunctionBuilder::new("claviron_budget_begin", begin_handler)
            .arg(Arg::new("seconds", DataType::Long))
            .returns(DataType::Void, false, false)
            .build()
            .map_err(|error| format!("claviron_budget_begin: {error}"))?,
    );
    entries.push(
        FunctionBuilder::new("claviron_budget_end", end_handler)
            .returns(DataType::Void, false, false)
            .build()
            .map_err(|error| format!("claviron_budget_end: {error}"))?,
    );
    Ok(())
}

extern "C" fn begin_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        retval.set_null();
        let mut seconds = Arg::new("seconds", DataType::Long);
        if let Err(error) = ex.parser().arg(&mut seconds).parse() {
            PhpException::from_message(format!(
                "claviron_budget_begin: {error}"
            ))
            .throw();
            return;
        }
        let seconds = match seconds.consume::<i64>() {
            Ok(seconds) if seconds > 0 => seconds as u64,
            _ => {
                PhpException::from_message(
                    "claviron_budget_begin: seconds must be a positive integer"
                        .to_owned(),
                )
                .throw();
                return;
            }
        };
        interrupt::arm(std::time::Duration::from_secs(seconds));
    }));
}

extern "C" fn end_handler(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        retval.set_null();
        interrupt::disarm();
    }));
}
