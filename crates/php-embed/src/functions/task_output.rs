//! Task-local console streams for embedded PHP programs.

use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, SapiGlobals};

use crate::engine::ClavironContext;
use crate::extension::FunctionEntries;
use crate::sapi::ServerContext;

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.extend([
        FunctionBuilder::new("claviron_task_output_write", write)
            .arg(Arg::new("stream", DataType::String))
            .arg(Arg::new("data", DataType::String))
            .returns(DataType::Long, false, false)
            .build()
            .map_err(|error| format!("claviron_task_output_write: {error}"))?,
        FunctionBuilder::new("claviron_task_output_flush", flush)
            .arg(Arg::new("stream", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_task_output_flush: {error}"))?,
    ]);
    Ok(())
}

fn throw(error: impl std::fmt::Display) {
    PhpException::from_message(format!("Claviron task output failed: {error}"))
        .throw();
}

fn context() -> Result<&'static mut ClavironContext, String> {
    let globals = SapiGlobals::get();
    if globals.server_context.is_null() {
        return Err(
            "task output requires an active Claviron execution".to_owned()
        );
    }
    Ok(ClavironContext::from_server_context(globals.server_context))
}

fn parse_stream(ex: &mut ExecuteData) -> Result<bool, String> {
    let mut stream = Arg::new("stream", DataType::String);
    ex.parser().arg(&mut stream).parse().map_err(|error| error.to_string())?;
    match stream
        .consume::<String>()
        .map_err(|_| "stream must be a string")?
        .as_str()
    {
        "stdout" => Ok(false),
        "stderr" => Ok(true),
        other => Err(format!("unknown stream '{other}'")),
    }
}

extern "C" fn write(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let mut stream = Arg::new("stream", DataType::String);
            let mut data = Arg::new("data", DataType::String);
            ex.parser()
                .arg(&mut stream)
                .arg(&mut data)
                .parse()
                .map_err(|error| error.to_string())?;
            let error = match stream
                .consume::<String>()
                .map_err(|_| "stream must be a string")?
                .as_str()
            {
                "stdout" => false,
                "stderr" => true,
                other => return Err(format!("unknown stream '{other}'")),
            };
            let data = data
                .zval()
                .and_then(|value| value.binary_slice::<u8>())
                .ok_or_else(|| "data must be a string".to_owned())?;
            let written =
                context()?.write_program_output(error, data).ok_or_else(
                    || "output is only available to embedded tasks".to_owned(),
                )?;
            if !written {
                return Err("write failed".to_owned());
            }
            i64::try_from(data.len()).map_err(|_| {
                "write length exceeds PHP integer range".to_owned()
            })
        })();
        match result {
            Ok(written) => retval.set_long(written),
            Err(error) => {
                retval.set_long(0);
                throw(error);
            }
        }
    }));
}

extern "C" fn flush(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = parse_stream(ex).and_then(|error| {
            context()?.flush_program_output(error).ok_or_else(|| {
                "output is only available to embedded tasks".to_owned()
            })
        });
        match result {
            Ok(flushed) => retval.set_bool(flushed),
            Err(error) => {
                retval.set_bool(false);
                throw(error);
            }
        }
    }));
}
