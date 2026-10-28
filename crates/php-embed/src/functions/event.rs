//! Framework-neutral PHP primitives for application events.

use claviron_app::{
    ApplicationId, ApplicationName, ApplicationRuntime, EventTopic, EventValue,
};
use ext_php_rs::args::Arg;
use ext_php_rs::builders::FunctionBuilder;
use ext_php_rs::class::RegisteredClass;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ExecuteData, SapiGlobals};

use crate::engine::{ClavironContext, PhpExecution};
use crate::event::{PhpEventObject, bridge, from_zval};
use crate::extension::FunctionEntries;
use crate::sapi::ServerContext;

pub(crate) fn append_function_entries(
    entries: &mut FunctionEntries
) -> Result<(), String> {
    entries.extend([
        FunctionBuilder::new("claviron_events_available", available_handler)
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_events_available: {error}"))?,
        FunctionBuilder::new("claviron_event_subscribe", subscribe_handler)
            .arg(Arg::new("topic", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_event_subscribe: {error}"))?,
        FunctionBuilder::new(
            "claviron_event_subscribe_many",
            subscribe_many_handler,
        )
        .arg(Arg::new("topics", DataType::Array))
        .returns(DataType::Long, false, false)
        .build()
        .map_err(|error| format!("claviron_event_subscribe_many: {error}"))?,
        FunctionBuilder::new("claviron_event_unsubscribe", unsubscribe_handler)
            .arg(Arg::new("topic", DataType::String))
            .returns(DataType::Bool, false, false)
            .build()
            .map_err(|error| format!("claviron_event_unsubscribe: {error}"))?,
        FunctionBuilder::new("claviron_event_publish", publish_handler)
            .arg(Arg::new("topic", DataType::String))
            .arg(Arg::new("payload", DataType::Mixed))
            .returns(DataType::Void, false, false)
            .build()
            .map_err(|error| format!("claviron_event_publish: {error}"))?,
        FunctionBuilder::new(
            "claviron_event_publish_shared",
            publish_shared_handler,
        )
        .arg(Arg::new("topic", DataType::String))
        .arg(Arg::new("payload", DataType::Mixed))
        .returns(DataType::Void, false, false)
        .build()
        .map_err(|error| format!("claviron_event_publish_shared: {error}"))?,
        FunctionBuilder::new("claviron_event_receive", receive_handler)
            .not_required()
            .arg(Arg::new("timeout_ms", DataType::Long).default("0"))
            .returns(DataType::object(PhpEventObject::CLASS_NAME), true, false)
            .build()
            .map_err(|error| format!("claviron_event_receive: {error}"))?,
    ]);
    Ok(())
}

extern "C" fn available_handler(
    _ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        retval.set_bool(crate::event::is_configured());
    }));
}

fn throw(error: impl std::fmt::Display) {
    PhpException::from_message(format!(
        "Claviron event operation failed: {error}"
    ))
    .throw();
}

pub(crate) fn context() -> Result<&'static mut ClavironContext, String> {
    let globals = SapiGlobals::get();
    if globals.server_context.is_null() {
        return Err(
            "PHP event operation requires an active Claviron execution"
                .to_owned(),
        );
    }
    Ok(ClavironContext::from_server_context(globals.server_context))
}

fn application(
    context: &ClavironContext
) -> Result<Option<ApplicationId>, String> {
    match &context.execution {
        PhpExecution::Program(program)
            if matches!(
                program.model,
                crate::engine::ProgramModel::Application
            ) =>
        {
            Ok(Some(ApplicationId::new(
                ApplicationRuntime::new("php")
                    .expect("static PHP runtime identity"),
                ApplicationName::new(&program.name)
                    .map_err(|error| error.to_string())?,
            )))
        }
        _ => Ok(None),
    }
}

fn parse_topic(ex: &mut ExecuteData) -> Result<String, String> {
    let mut topic = Arg::new("topic", DataType::String);
    ex.parser().arg(&mut topic).parse().map_err(|error| error.to_string())?;
    let topic = topic
        .consume::<String>()
        .map_err(|_| "PHP event topic must be a string".to_owned())?;
    EventTopic::new(topic)
        .map(|topic| topic.to_string())
        .map_err(|error| error.to_string())
}

fn parse_topics(ex: &mut ExecuteData) -> Result<Vec<String>, String> {
    let mut topics = Arg::new("topics", DataType::Array);
    ex.parser().arg(&mut topics).parse().map_err(|error| error.to_string())?;
    let topics = topics
        .zval()
        .and_then(|value| value.array())
        .ok_or_else(|| "PHP event topics must be an array".to_owned())?;
    topics
        .iter()
        .map(|(_, topic)| {
            let topic = topic.binary_slice::<u8>().ok_or_else(|| {
                "every PHP event topic must be a string".to_owned()
            })?;
            let topic = std::str::from_utf8(topic).map_err(|_| {
                "PHP event topics must be valid UTF-8".to_owned()
            })?;
            EventTopic::new(topic)
                .map(|topic| topic.to_string())
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn parse_event(ex: &mut ExecuteData) -> Result<(String, EventValue), String> {
    let mut topic = Arg::new("topic", DataType::String);
    let mut payload = Arg::new("payload", DataType::Mixed);
    ex.parser()
        .arg(&mut topic)
        .arg(&mut payload)
        .parse()
        .map_err(|error| error.to_string())?;
    let topic = topic
        .consume::<String>()
        .map_err(|_| "PHP event topic must be a string".to_owned())?;
    let payload = payload
        .zval()
        .ok_or_else(|| "PHP event payload is missing".to_owned())
        .and_then(|value| from_zval(value))?;
    let topic = EventTopic::new(topic).map_err(|error| error.to_string())?;
    Ok((topic.to_string(), payload))
}

extern "C" fn subscribe_many_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            // Validate the complete list before mutating the execution subscription.
            let topics = parse_topics(ex)?;
            let bridge = bridge()?;
            let context = context()?;
            let application = application(context)?;
            let subscription = context
                .event_subscription
                .get_or_insert_with(|| bridge.subscription(application));
            i64::try_from(subscription.subscribe_many(topics)).map_err(|_| {
                "PHP event subscription count exceeds integer range".to_owned()
            })
        })();
        match result {
            Ok(inserted) => retval.set_long(inserted),
            Err(error) => {
                retval.set_long(0);
                throw(error);
            }
        }
    }));
}

extern "C" fn subscribe_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let topic = parse_topic(ex)?;
            let bridge = bridge()?;
            let context = context()?;
            let application = application(context)?;
            let subscription = context
                .event_subscription
                .get_or_insert_with(|| bridge.subscription(application));
            Ok::<_, String>(subscription.subscribe(topic))
        })();
        match result {
            Ok(inserted) => retval.set_bool(inserted),
            Err(error) => {
                retval.set_bool(false);
                throw(error);
            }
        }
    }));
}

extern "C" fn unsubscribe_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let topic = parse_topic(ex)?;
            let context = context()?;
            let subscription = &mut context.event_subscription;
            let removed = subscription
                .as_mut()
                .is_some_and(|subscription| subscription.unsubscribe(&topic));
            if subscription
                .as_ref()
                .is_some_and(|subscription| subscription.is_empty())
            {
                subscription.take();
            }
            Ok::<_, String>(removed)
        })();
        match result {
            Ok(removed) => retval.set_bool(removed),
            Err(error) => {
                retval.set_bool(false);
                throw(error);
            }
        }
    }));
}

fn publish(ex: &mut ExecuteData) -> Result<(), String> {
    let (topic, payload) = parse_event(ex)?;
    let application = application(context()?)?.ok_or_else(|| {
        "application-scoped PHP events require a named persistent application".to_owned()
    })?;
    bridge()?.publish_for_application(application, topic, payload)
}

fn publish_shared(ex: &mut ExecuteData) -> Result<(), String> {
    let (topic, payload) = parse_event(ex)?;
    bridge()?.publish_shared(topic, payload)
}

extern "C" fn publish_handler(
    ex: &mut ExecuteData,
    _retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        if let Err(error) = publish(ex) {
            throw(error);
        }
    }));
}

extern "C" fn publish_shared_handler(
    ex: &mut ExecuteData,
    _retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        if let Err(error) = publish_shared(ex) {
            throw(error);
        }
    }));
}

extern "C" fn receive_handler(
    ex: &mut ExecuteData,
    retval: &mut Zval,
) {
    ext_php_rs::zend::run_handler(std::panic::AssertUnwindSafe(|| {
        let result = (|| {
            let mut timeout = Arg::new("timeout_ms", DataType::Long);
            ex.parser()
                .not_required()
                .arg(&mut timeout)
                .parse()
                .map_err(|error| error.to_string())?;
            let timeout = match timeout.zval() {
                None => 0,
                Some(_) => timeout.consume::<i64>().map_err(|_| {
                    "PHP event timeout must be an integer".to_owned()
                })?,
            };
            let timeout = u64::try_from(timeout).map_err(|_| {
                "PHP event timeout must not be negative".to_owned()
            })?;
            let bridge = bridge()?;
            let context = context()?;
            let cancellation = match &context.execution {
                PhpExecution::Program(program) => {
                    Some(program.cancellation.clone())
                }
                PhpExecution::Request(_) => None,
            };
            let subscription =
                context.event_subscription.as_mut().ok_or_else(|| {
                    "subscribe to at least one PHP event topic before receiving"
                        .to_owned()
                })?;
            bridge.receive(subscription, timeout, cancellation.as_ref())
        })();

        match result {
            Ok(Some(event)) => {
                if let Err(error) = event.set_zval(retval) {
                    retval.set_null();
                    throw(error);
                }
            }
            Ok(None) => retval.set_null(),
            Err(error) => {
                retval.set_null();
                throw(error);
            }
        }
    }));
}
