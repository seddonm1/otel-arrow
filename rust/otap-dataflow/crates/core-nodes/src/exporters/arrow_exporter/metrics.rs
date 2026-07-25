// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the Arrow IPC object-store exporter.

use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry_macros::metric_set;

/// Arrow exporter object-write metrics.
#[metric_set(name = "otap.exporter.arrow")]
#[derive(Debug, Default, Clone)]
pub struct ArrowExporterMetrics {
    /// Number of Arrow IPC objects written by completed pdata exports.
    #[metric(unit = "{object}")]
    pub objects_written: Counter<u64>,

    /// Number of Arrow IPC bytes written by completed pdata exports.
    #[metric(unit = "By")]
    pub bytes_written: Counter<u64>,
}
