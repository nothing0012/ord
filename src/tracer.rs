use log::{info, warn};
use opentelemetry::{
  sdk::trace::{RandomIdGenerator, Sampler},
  trace::TraceError,
};
use opentelemetry_datadog::ApiVersion;
use std::env;

fn get_trace_sample_rate() -> f64 {
  let default_sample_rate = 0.1;
  let rate = env::var("DD_TRACE_SAMPLE_RATE")
    .unwrap_or("0.1".to_owned())
    .parse::<f64>()
    .unwrap_or_else(|e| {
      warn!(
        "failed to parse DD_TRACE_SAMPLE_RATE: {}, defaulting to 0.1 sample rate",
        e
      );
      default_sample_rate
    });

  if (0.0..=1.0).contains(&rate) {
    rate
  } else {
    warn!("DD_TRACE_SAMPLE_RATE must be between 0 and 1, defaulting to 0.1 sample rate");
    default_sample_rate
  }
}

fn get_agent_url() -> String {
  let port = env::var("DD_TRACE_AGENT_PORT")
    .unwrap_or("8126".to_string())
    .parse::<u64>()
    .unwrap_or_else(|_| {
      warn!("Failed to parse DD_TRACE_AGENT_PORT, defaulting to 8126");
      8126
    });
  let host = env::var("DD_TRACE_AGENT_HOSTNAME").unwrap_or("localhost".to_owned());

  format!("http://{host}:{port}")
}

pub(crate) fn init() -> Result<opentelemetry::sdk::trace::Tracer, TraceError> {
  opentelemetry_datadog::new_pipeline()
    .with_service_name(env::var("DD_SERVICE").unwrap_or("ord-kafka".to_owned()))
    .with_api_version(ApiVersion::Version05)
    .with_agent_endpoint(get_agent_url())
    .with_trace_config(
      opentelemetry::sdk::trace::config()
        .with_sampler({
          let sample_rate = get_trace_sample_rate();
          if sample_rate == 0.0 {
            Sampler::AlwaysOff
          } else if sample_rate == 1.0 {
            Sampler::AlwaysOn
          } else {
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sample_rate)))
          }
        })
        .with_id_generator(RandomIdGenerator::default()),
    )
    .install_simple()
}

pub(crate) fn close() {
  info!("Shutting down tracer...");
  opentelemetry::global::shutdown_tracer_provider();
  info!("Tracer shutdown complete.");
}
