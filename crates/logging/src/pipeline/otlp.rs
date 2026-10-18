//! The OTLP channel adapter: records as OpenTelemetry logs.
//!
//! Built on OpenTelemetry's logs bridge. The writer hands each record to the
//! SDK's batch processor, which queues it and returns; sending happens on the
//! processor's own thread, in batches, with the SDK's documented batch
//! interval and export timeout. A queue that fills — an endpoint that is slow
//! or gone — drops and counts, so a network sink never holds up the file
//! channels beside it.

use std::time::{Duration, SystemTime};

use opentelemetry::logs::{
    AnyValue, LogRecord, Logger, LoggerProvider, Severity,
};
use opentelemetry_otlp::{LogExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{
    BatchConfigBuilder, BatchLogProcessor, SdkLogger, SdkLoggerProvider,
};

use super::record::{Decoded, Level, OwnedValue};

pub(crate) struct OtlpSink {
    provider: SdkLoggerProvider,
    logger: SdkLogger,
}

impl OtlpSink {
    pub(crate) fn open(
        endpoint: &str,
        queue: usize,
        service: &str,
    ) -> Result<Self, String> {
        let exporter = LogExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|error| {
                format!("OTLP exporter for {endpoint}: {error}")
            })?;
        let processor = BatchLogProcessor::builder(exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(queue)
                    .build(),
            )
            .build();
        let provider = SdkLoggerProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name(service.to_owned())
                    .build(),
            )
            .with_log_processor(processor)
            .build();
        let logger = provider.logger("claviron");
        Ok(Self { provider, logger })
    }

    pub(crate) fn emit(
        &self,
        channel: &str,
        node: u16,
        record: &Decoded,
    ) {
        let mut log = self.logger.create_log_record();
        let (severity, text) = match record.level {
            Level::Error => (Severity::Error, "ERROR"),
            Level::Warn => (Severity::Warn, "WARN"),
            Level::Info => (Severity::Info, "INFO"),
            Level::Debug => (Severity::Debug, "DEBUG"),
            Level::Trace => (Severity::Trace, "TRACE"),
        };
        log.set_severity_number(severity);
        log.set_severity_text(text);
        let at = if record.time >= 0 {
            SystemTime::UNIX_EPOCH + Duration::from_nanos(record.time as u64)
        } else {
            SystemTime::UNIX_EPOCH
        };
        log.set_timestamp(at);
        log.set_observed_timestamp(SystemTime::now());
        if !record.message.is_empty() {
            log.set_body(AnyValue::String(record.message.clone().into()));
        }
        log.add_attribute("channel", channel.to_owned());
        log.add_attribute("node", i64::from(node));
        if !record.target.is_empty() {
            log.add_attribute("target", record.target.clone());
        }
        for (key, value) in &record.fields {
            let value = match value {
                OwnedValue::Str(text) => AnyValue::String(text.clone().into()),
                OwnedValue::U64(number) => {
                    AnyValue::Int(i64::try_from(*number).unwrap_or(i64::MAX))
                }
                OwnedValue::I64(number) => AnyValue::Int(*number),
                OwnedValue::F64(number) => AnyValue::Double(*number),
                OwnedValue::Bool(flag) => AnyValue::Boolean(*flag),
            };
            log.add_attribute(key.clone(), value);
        }
        if record.truncated {
            log.add_attribute("truncated", true);
        }
        self.logger.emit(log);
    }

    /// Send what is queued and stop the processor's thread.
    pub(crate) fn shutdown(&self) {
        if let Err(error) = self.provider.shutdown() {
            tracing::warn!(%error, "OTLP log export did not shut down cleanly");
        }
    }
}
