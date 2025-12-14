use tracing::{Subscriber, subscriber::set_global_default};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::{
    EnvFilter, Registry,
    fmt::{MakeWriter, time::UtcTime},
    layer::{Layered, SubscriberExt},
};

use crate::error::BlinkyError;

pub fn build_json_subscriber<Sink>(
    name: impl Into<String>,
    env_filter: impl AsRef<str>,
    sink: Sink,
) -> impl Subscriber + Send + Sync
where
    Sink: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let skipped_fields = vec!["target", "line", "file"];
    let formatting_layer = BunyanFormattingLayer::new(name.into(), sink)
        .skip_fields(skipped_fields.into_iter())
        .expect("Specified fields in `BunyanFormattingLayer` cannot be skipped");
    layered_subscriber_with_env_filter(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer)
}

pub fn build_plain_subscriber<Sink>(
    env_filter: impl AsRef<str>,
    sink: Sink,
) -> impl Subscriber + Send + Sync
where
    Sink: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let formatting_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(false) // cleaner logs in containers
        .with_file(false)
        .with_line_number(false)
        .with_timer(UtcTime::rfc_3339())
        .with_writer(sink);

    layered_subscriber_with_env_filter(env_filter).with(formatting_layer)
}

pub fn try_init_subscriber(subscriber: impl Subscriber + Send + Sync) -> Result<(), BlinkyError> {
    LogTracer::init()?;
    set_global_default(subscriber)?;

    Ok(())
}

fn layered_subscriber_with_env_filter(env_filter: impl AsRef<str>) -> Layered<EnvFilter, Registry> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));
    Registry::default().with(env_filter)
}
