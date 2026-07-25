// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Arrow IPC object-store exporter.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otap_df_engine::error::{Error, ExporterErrorKind, format_error_sources};
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{ExporterInbox, Message};
use otap_df_engine::node::NodeId;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otap_df_otap::OTAP_EXPORTER_FACTORIES;
use otap_df_otap::metrics::ExporterPDataExportMetrics;
use otap_df_otap::pdata::OtapPdata;
use otap_df_pdata::TryIntoWithOptions;
use otap_df_pdata::otap::OtapArrowRecords;
use otap_df_telemetry::common_attributes::{Outcome, SignalOutcomeAttributes};
use otap_df_telemetry::metrics::{MeasurementMetricSet, MetricSet, MetricSetHandler};

pub mod config;
pub mod metrics;
pub mod writer;

use config::Config;
use metrics::ArrowExporterMetrics;
use writer::{ArrowWriter, ArrowWriterError};

/// URN for the Arrow IPC object-store exporter.
pub const ARROW_EXPORTER_URN: &str = "urn:otel:exporter:arrow";

/// Exports each pdata batch as standalone Arrow IPC objects.
pub struct ArrowExporter {
    config: Config,
    pdata_metrics: MeasurementMetricSet<ExporterPDataExportMetrics>,
    io_metrics: MetricSet<ArrowExporterMetrics>,
}

impl ArrowExporter {
    fn terminal_state(&mut self, deadline: Instant) -> TerminalState {
        let mut snapshots = self.pdata_metrics.terminal_snapshots();
        if self.io_metrics.needs_flush() {
            snapshots.extend(self.io_metrics.terminal_snapshots());
        }
        TerminalState::new(deadline, snapshots)
    }
}

/// Declares the Arrow exporter as a local exporter factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static ARROW_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: ARROW_EXPORTER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
        let config = serde_json::from_value(node_config.config.clone()).map_err(|source| {
            ConfigError::InvalidUserConfig {
                error: format!("failed to parse Arrow exporter configuration: {source}"),
            }
        })?;
        Ok(ExporterWrapper::local(
            ArrowExporter {
                config,
                pdata_metrics: ExporterPDataExportMetrics::register(&pipeline),
                io_metrics: pipeline.register_metrics::<ArrowExporterMetrics>(),
            },
            node,
            node_config,
            exporter_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otap_df_config::validation::validate_typed_config::<Config>,
};

#[async_trait(?Send)]
impl Exporter<OtapPdata> for ArrowExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let writer = ArrowWriter::from_config(&self.config).map_err(|source| {
            let source_detail = format_error_sources(&source);
            Error::ExporterError {
                exporter: effect_handler.exporter_id(),
                kind: ExporterErrorKind::Configuration,
                error: format!("failed to initialize Arrow exporter storage: {source}"),
                source_detail,
            }
        })?;

        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                }) => {
                    _ = metrics_reporter.report_measurement(&mut self.pdata_metrics);
                    _ = metrics_reporter.report(&mut self.io_metrics);
                }
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    return Ok(self.terminal_state(deadline));
                }
                Message::PData(pdata) => {
                    export_pdata(
                        &writer,
                        pdata,
                        &effect_handler,
                        &mut self.pdata_metrics,
                        &mut self.io_metrics,
                    )
                    .await?;
                }
                _ => {}
            }
        }
    }
}

async fn export_pdata(
    writer: &ArrowWriter,
    pdata: OtapPdata,
    effect_handler: &EffectHandler<OtapPdata>,
    pdata_metrics: &mut MeasurementMetricSet<ExporterPDataExportMetrics>,
    io_metrics: &mut MetricSet<ArrowExporterMetrics>,
) -> Result<(), Error> {
    let signal_type = pdata.signal_type();
    let idempotency_key = pdata.idempotency_key();
    let records: OtapArrowRecords = match pdata.payload_ref().clone().try_into_with_default() {
        Ok(records) => records,
        Err(source) => {
            record_pdata_outcome(pdata_metrics, signal_type, Outcome::Failure);
            effect_handler
                .notify_nack(NackMsg::new_permanent(
                    format!("failed to convert pdata to OTAP Arrow records: {source}"),
                    pdata,
                ))
                .await?;
            return Ok(());
        }
    };

    match writer.write_pdata(idempotency_key, &records).await {
        Ok(stats) => {
            record_write_metrics(io_metrics, stats);
            record_pdata_outcome(pdata_metrics, signal_type, Outcome::Success);
            effect_handler.notify_ack(AckMsg::new(pdata)).await
        }
        Err(source) => {
            record_pdata_outcome(pdata_metrics, signal_type, Outcome::Failure);
            let reason = format!("failed to write Arrow IPC pdata objects: {source}");
            if is_permanent(&source) {
                effect_handler
                    .notify_nack(NackMsg::new_permanent(reason, pdata))
                    .await
            } else {
                effect_handler
                    .notify_nack(NackMsg::new(reason, pdata))
                    .await
            }
        }
    }
}

fn record_pdata_outcome(
    metrics: &mut MeasurementMetricSet<ExporterPDataExportMetrics>,
    signal: SignalType,
    outcome: Outcome,
) {
    metrics
        .with(SignalOutcomeAttributes { signal, outcome })
        .messages
        .inc();
}

fn record_write_metrics(metrics: &mut MetricSet<ArrowExporterMetrics>, stats: writer::WriteStats) {
    if stats.objects_written > 0 {
        metrics.objects_written.add(stats.objects_written);
    }
    if stats.bytes_written > 0 {
        metrics.bytes_written.add(stats.bytes_written);
    }
}

const fn is_permanent(error: &ArrowWriterError) -> bool {
    matches!(
        error,
        ArrowWriterError::InvalidIdempotencyKey { .. }
            | ArrowWriterError::InvalidObjectPath { .. }
            | ArrowWriterError::Encode { .. }
    )
}

#[cfg(test)]
mod tests {
    use otap_df_engine::context::ControllerContext;
    use otap_df_telemetry::metrics::MetricValue;
    use otap_df_telemetry::registry::TelemetryRegistryHandle;

    use super::*;

    fn test_pipeline_context() -> PipelineContext {
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry);
        controller.pipeline_context_with("group".into(), "pipeline".into(), 0, 1, 0)
    }

    /// Scenario: Successful and failed pdata exports update Arrow exporter metrics before shutdown.
    /// Guarantees: Terminal handoff contains signal outcomes plus completed object and byte totals.
    #[test]
    fn terminal_state_hands_off_export_metrics() {
        let pipeline = test_pipeline_context();
        let mut exporter = ArrowExporter {
            config: Config {
                storage: otap_df_otap::object_store::StorageType::File {
                    base_uri: "/tmp/arrow-files".to_string(),
                },
                retry: None,
                fsync: true,
                compression: config::Compression::default(),
                object_path_template: config::ObjectPathTemplate::default(),
            },
            pdata_metrics: ExporterPDataExportMetrics::register(&pipeline),
            io_metrics: pipeline.register_metrics::<ArrowExporterMetrics>(),
        };
        record_pdata_outcome(
            &mut exporter.pdata_metrics,
            SignalType::Traces,
            Outcome::Success,
        );
        record_pdata_outcome(
            &mut exporter.pdata_metrics,
            SignalType::Logs,
            Outcome::Failure,
        );
        record_write_metrics(
            &mut exporter.io_metrics,
            writer::WriteStats {
                objects_written: 3,
                bytes_written: 2_048,
            },
        );

        let deadline = Instant::now();
        let snapshots = exporter.terminal_state(deadline).into_metrics();

        assert_eq!(snapshots.len(), 3);
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.descriptor().name == "exporter.pdata.exports"
                && snapshot.measurement_attribute_value("signal") == Some("traces")
                && snapshot.measurement_attribute_value("outcome") == Some("success")
                && snapshot.get_metrics() == [MetricValue::U64(1)]
        }));
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.descriptor().name == "exporter.pdata.exports"
                && snapshot.measurement_attribute_value("signal") == Some("logs")
                && snapshot.measurement_attribute_value("outcome") == Some("failure")
                && snapshot.get_metrics() == [MetricValue::U64(1)]
        }));
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.descriptor().name == "otap.exporter.arrow"
                && snapshot.get_metrics() == [MetricValue::U64(3), MetricValue::U64(2_048)]
        }));
        assert!(exporter.terminal_state(deadline).is_empty());
    }
}
