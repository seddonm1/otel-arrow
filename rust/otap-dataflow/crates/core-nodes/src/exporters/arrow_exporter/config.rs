// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the Arrow IPC object-store exporter.

use object_store::path::Path;
use serde::{Deserialize, Deserializer};

/// Default object path template.
pub const DEFAULT_OBJECT_PATH_TEMPLATE: &str =
    "otap/v1/signal=[signal]/payload=[payload]/date=[date]/hour=[hour]/[pdata_id].arrow";

const SUPPORTED_PLACEHOLDERS: &[&str] = &[
    "signal", "payload", "pdata_id", "date", "year", "month", "day", "hour", "minute",
];

/// Compression applied to Arrow IPC record-batch buffers.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// Write uncompressed Arrow IPC buffers.
    None,
    /// Compress Arrow IPC buffers with Zstandard.
    #[default]
    Zstd,
}

impl Compression {
    pub(crate) const fn ipc_type(self) -> Option<arrow_ipc::CompressionType> {
        match self {
            Self::None => None,
            Self::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
        }
    }
}

/// Validated template used to construct an object path for each Arrow payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectPathTemplate(String);

impl ObjectPathTemplate {
    /// Parses and validates an object path template.
    ///
    /// # Errors
    ///
    /// Returns an error when the template contains unsupported placeholders,
    /// or cannot produce a safe object-store path.
    pub fn parse(template: impl Into<String>) -> Result<Self, String> {
        let template = template.into();
        validate_template(&template)?;
        Ok(Self(template))
    }

    /// Returns the original validated template.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ObjectPathTemplate {
    fn default() -> Self {
        Self(DEFAULT_OBJECT_PATH_TEMPLATE.to_string())
    }
}

impl<'de> Deserialize<'de> for ObjectPathTemplate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let template = String::deserialize(deserializer)?;
        Self::parse(template).map_err(serde::de::Error::custom)
    }
}

/// Configuration for the Arrow object-store exporter.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Destination for standalone Arrow IPC pdata payload objects.
    pub storage: otap_df_otap::object_store::StorageType,

    /// Optional retry settings for cloud-backed object storage.
    pub retry: Option<otap_df_otap::object_store::RetryOptions>,

    /// Whether local files and supported directory entries are synced before success.
    ///
    /// This setting only applies when `storage` is `file`.
    #[serde(default = "default_fsync")]
    pub fsync: bool,

    /// Compression applied to Arrow IPC record-batch buffers.
    #[serde(default)]
    pub compression: Compression,

    /// Template used to construct each Arrow IPC object path.
    #[serde(default)]
    pub object_path_template: ObjectPathTemplate,
}

const fn default_fsync() -> bool {
    true
}

fn validate_template(template: &str) -> Result<(), String> {
    if template.is_empty() {
        return Err("object path template cannot be empty".to_string());
    }
    if template.starts_with('/') || template.starts_with('\\') {
        return Err("object path template must be relative".to_string());
    }
    if template.contains('\\') {
        return Err("object path template cannot contain backslashes".to_string());
    }
    if !template.ends_with(".arrow") {
        return Err("object path template must end with .arrow".to_string());
    }

    parse_placeholders(template)?;

    let example_path = render_example(template);
    let _ = Path::parse(&example_path)
        .map_err(|source| format!("object path template produces an invalid path: {source}"))?;
    Ok(())
}

fn parse_placeholders(template: &str) -> Result<(), String> {
    let mut remainder = template;

    while let Some(open) = remainder.find('[') {
        if remainder[..open].contains(']') {
            return Err("object path template contains an unmatched ']'".to_string());
        }

        let after_open = &remainder[open + 1..];
        let close = after_open
            .find(']')
            .ok_or_else(|| "object path template contains an unmatched '['".to_string())?;
        let placeholder = &after_open[..close];
        if !SUPPORTED_PLACEHOLDERS.contains(&placeholder) {
            return Err(format!(
                "unsupported object path placeholder [{placeholder}]"
            ));
        }

        remainder = &after_open[close + 1..];
    }

    if remainder.contains(']') {
        return Err("object path template contains an unmatched ']'".to_string());
    }

    Ok(())
}

fn render_example(template: &str) -> String {
    template
        .replace("[signal]", "logs")
        .replace("[payload]", "resource_attrs")
        .replace("[pdata_id]", "019f9bd8-0446-78f0-b8b9-c662301b9876")
        .replace("[date]", "2026-07-26")
        .replace("[year]", "2026")
        .replace("[month]", "07")
        .replace("[day]", "26")
        .replace("[hour]", "00")
        .replace("[minute]", "34")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Scenario: Local Arrow exporter configuration omits the fsync setting.
    /// Guarantees: Durable local publication is enabled by default.
    #[test]
    fn local_fsync_defaults_to_true() {
        let config: Config = serde_json::from_value(json!({
            "storage": {
                "file": {
                    "base_uri": "/tmp/arrow-files"
                }
            },
            "retry": null
        }))
        .expect("deserialize config");

        assert!(config.fsync);
        assert_eq!(config.compression, Compression::Zstd);
        assert_eq!(
            config.object_path_template.as_str(),
            DEFAULT_OBJECT_PATH_TEMPLATE
        );
    }

    /// Scenario: An operator explicitly disables local fsync.
    /// Guarantees: Configuration preserves the requested non-durable local mode.
    #[test]
    fn local_fsync_can_be_disabled() {
        let config: Config = serde_json::from_value(json!({
            "storage": {
                "file": {
                    "base_uri": "/tmp/arrow-files"
                }
            },
            "retry": null,
            "fsync": false
        }))
        .expect("deserialize config");

        assert!(!config.fsync);
    }

    /// Scenario: An operator disables Arrow IPC buffer compression.
    /// Guarantees: The `none` setting selects uncompressed Arrow IPC output.
    #[test]
    fn arrow_ipc_compression_can_be_disabled() {
        let config: Config = serde_json::from_value(json!({
            "storage": {
                "file": {
                    "base_uri": "/tmp/arrow-files"
                }
            },
            "retry": null,
            "compression": "none"
        }))
        .expect("deserialize config");

        assert_eq!(config.compression, Compression::None);
    }

    /// Scenario: An operator configures an unsupported Arrow IPC compression codec.
    /// Guarantees: Configuration accepts only `zstd` and `none`.
    #[test]
    fn rejects_unsupported_arrow_ipc_compression() {
        let result = serde_json::from_value::<Config>(json!({
            "storage": {
                "file": {
                    "base_uri": "/tmp/arrow-files"
                }
            },
            "retry": null,
            "compression": "gzip"
        }));

        assert!(result.is_err());
    }

    /// Scenario: An operator configures all supported object path placeholders.
    /// Guarantees: Valid deterministic partition and identity fields are accepted.
    #[test]
    fn accepts_supported_object_path_placeholders() {
        let config: Config = serde_json::from_value(json!({
            "storage": {
                "file": {
                    "base_uri": "/tmp/arrow-files"
                }
            },
            "retry": null,
            "object_path_template": "[year]-[month]-[day]/hour=[hour]/minute=[minute]/signal=[signal]/payload=[payload]/[pdata_id].arrow"
        }))
        .expect("deserialize config");

        assert!(config.object_path_template.as_str().contains("[minute]"));
    }

    /// Scenario: Object path templates omit payload or pdata identity placeholders.
    /// Guarantees: Operators may choose object naming and collision semantics.
    #[test]
    fn accepts_object_path_templates_without_identity_placeholders() {
        for template in ["[payload]/file.arrow", "[pdata_id].arrow", "file.arrow"] {
            let _template =
                ObjectPathTemplate::parse(template).expect("template should be accepted");
        }
    }

    /// Scenario: Object path templates contain unsafe syntax.
    /// Guarantees: Configuration rejects traversal, absolute paths, and malformed templates.
    #[test]
    fn rejects_unsafe_object_path_templates() {
        for template in [
            "/[payload]/[pdata_id].arrow",
            "../[payload]/[pdata_id].arrow",
            "[payload]/../[pdata_id].arrow",
            "[payload]\\[pdata_id].arrow",
            "[payload]/[unknown]/[pdata_id].arrow",
            "[payload]/[pdata_id",
            "[payload]/[pdata_id].ipc",
        ] {
            assert!(
                ObjectPathTemplate::parse(template).is_err(),
                "template should be rejected: {template}"
            );
        }
    }
}
