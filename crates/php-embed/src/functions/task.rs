//! One task-owned wait for application events and invocations.
//! No PHP callbacks run on Tokio threads and no new transport is introduced.

use claviron_orbit::Invocation;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::{ZendClassObject, Zval};
use ext_php_rs::zend::ExecuteData;

use crate::engine::{PhpExecution, ProgramModel};
use crate::event::ReceivedEvent;
use crate::extension::FunctionEntries;
use crate::functions::{event::context, orbit::invoke};

enum TaskInput {
    Event(ReceivedEvent),
    Invocation(Invocation),
}

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.push(
        FunctionBuilder::new("claviron_task_receive", receive_handler)
            .returns(DataType::Mixed, true, false)
            .build()
            .map_err(|error| format!("claviron_task_receive: {error}"))?,
    );
    Ok(())
}

extern "C" fn receive_handler(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = receive().and_then(|input| match input {
            Some(TaskInput::Event(event)) => event.set_zval(retval),
            Some(TaskInput::Invocation(invocation)) => ZendClassObject::new(
                invoke::PhpInvocationObject::from(invocation),
            )
            .set_zval(retval, false)
            .map_err(|error| error.to_string()),
            None => {
                retval.set_null();
                Ok(())
            }
        });
        if let Err(error) = result {
            retval.set_null();
            PhpException::from_message(format!(
                "Claviron task receive failed: {error}"
            ))
            .throw();
        }
    }));
}

fn receive() -> Result<Option<TaskInput>, String> {
    let context = context()?;
    let cancellation = match &context.execution {
        PhpExecution::Program(program)
            if program.model == ProgramModel::Task =>
        {
            program.cancellation.clone()
        }
        _ => {
            return Err(
                "claviron_task_receive requires a PHP runtime Task".into()
            );
        }
    };
    let mut events = context
        .event_subscription
        .lock()
        .map_err(|_| "PHP event subscription lock is poisoned".to_owned())?;
    let mut invocations =
        context.invocation_subscription.lock().map_err(|_| {
            "PHP invocation subscription lock is poisoned".to_owned()
        })?;
    let has_events = events.as_ref().is_some_and(|events| !events.is_empty());
    let has_invocations =
        invocations.as_ref().is_some_and(|invocations| !invocations.is_empty());
    if !has_events && !has_invocations {
        return Err("subscribe to an event topic or invocation operation before receiving".into());
    }
    invoke::bridge()?.block_on(async {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(None),
            input = async {
                tokio::select! {
                    event = async {
                        match events.as_mut() {
                            Some(events) if has_events => events.receive().await,
                            _ => std::future::pending().await,
                        }
                    } => event.map(TaskInput::Event),
                    invocation = async {
                        match invocations.as_mut() {
                            Some(invocations) if has_invocations => invocations.receive().await,
                            _ => std::future::pending().await,
                        }
                    } => invocation.map(TaskInput::Invocation),
                }
            } => input.map(Some),
        }
    })?
}
