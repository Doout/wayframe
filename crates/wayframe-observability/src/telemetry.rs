//! Optional bounded OTLP/HTTP export; logging filters do not disable trace spans.
use anyhow::{ensure, Result};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider},
    Resource,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

pub struct Telemetry(Option<SdkTracerProvider>);
impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(provider) = &self.0 {
            if let Err(error) = provider.shutdown() {
                eprintln!("trace exporter shutdown failed: {error}");
            }
        }
    }
}
pub fn init() -> Result<Telemetry> {
    let enabled = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some();
    let provider = if enabled {
        let protocol = std::env::var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL"))
            .unwrap_or_else(|_| "http/protobuf".into());
        let protocol = match protocol.as_str() {
            "http/protobuf" => Protocol::HttpBinary,
            "http/json" => Protocol::HttpJson,
            _ => anyhow::bail!("trace exporter supports http/protobuf or http/json"),
        };
        let sampler = match std::env::var("OTEL_TRACES_SAMPLER")
            .unwrap_or_else(|_| "traceidratio".into())
            .as_str()
        {
            "always_on" => Sampler::AlwaysOn,
            "always_off" => Sampler::AlwaysOff,
            "traceidratio" => {
                let ratio: f64 = std::env::var("OTEL_TRACES_SAMPLER_ARG")
                    .unwrap_or_else(|_| "1".into())
                    .parse()?;
                ensure!(
                    ratio.is_finite() && (0.0..=1.0).contains(&ratio),
                    "invalid trace sampling ratio"
                );
                Sampler::TraceIdRatioBased(ratio)
            }
            _ => anyhow::bail!(
                "unsupported trace sampler; use always_on, always_off or traceidratio"
            ),
        };
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(protocol)
            .build()?;
        let processor = BatchSpanProcessor::builder(exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(4096)
                    .with_max_export_batch_size(512)
                    .build(),
            )
            .build();
        Some(
            SdkTracerProvider::builder()
                .with_span_processor(processor)
                .with_sampler(sampler)
                .with_resource(
                    Resource::builder()
                        .with_service_name(
                            std::env::var("OTEL_SERVICE_NAME")
                                .unwrap_or_else(|_| "wayframe-gateway".into()),
                        )
                        .build(),
                )
                .build(),
        )
    } else {
        None
    };
    let layer = provider.as_ref().map(|p| {
        tracing_opentelemetry::layer()
            .with_tracer(p.tracer("wayframe"))
            .with_filter(tracing_subscriber::filter::LevelFilter::INFO)
    });
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                ),
        )
        .with(layer)
        .try_init()?;
    Ok(Telemetry(provider))
}
