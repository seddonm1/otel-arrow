# Arrow Object-Store Exporter

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `exporter:arrow` (`urn:otel:exporter:arrow`)
- Feature gate: Default; S3 requires `aws`, Azure requires `azure`
- Stability: Experimental

## Overview

The Arrow object-store exporter converts each received OTAP pdata batch to Arrow records and exports every populated payload as a standalone Arrow IPC file. This provides a durable, typed snapshot that is more efficient to store and query than JSON and can be read directly by Arrow-native query engines before later compaction to Parquet.

## Getting Started

### Local filesystem

Write Arrow IPC files to a local directory:

```yaml
type: exporter:arrow
config:
  storage:
    file:
      base_uri: "/var/lib/otel-arrow"
  fsync: true
  compression: zstd
  object_path_template: "otap/v1/signal=[signal]/payload=[payload]/date=[date]/hour=[hour]/[pdata_id].arrow"
```

### Amazon S3

Write to an S3 bucket using the standard AWS credential chain:

```yaml
type: exporter:arrow
config:
  storage:
    s3:
      base_uri: "s3://otel-archive/arrow"
      region: "us-east-1"
      auth:
        type: default
  compression: zstd
  object_path_template: "otap/v1/signal=[signal]/payload=[payload]/date=[date]/hour=[hour]/[pdata_id].arrow"
```

S3-compatible stores can also set `endpoint`, `allow_http`, and `virtual_hosted_style_request` on the `s3` storage configuration.

### Azure

Write to Azure Blob Storage using workload identity:

```yaml
type: exporter:arrow
config:
  storage:
    azure:
      base_uri: "https://account.blob.core.windows.net/container/arrow"
      auth:
        type: workload_identity
  compression: zstd
  object_path_template: "otap/v1/signal=[signal]/payload=[payload]/date=[date]/hour=[hour]/[pdata_id].arrow"
```

## Configuration

The `fsync` option applies only to local file storage and defaults to `true`. The `compression` option controls Arrow IPC record-batch buffer compression. It defaults to `zstd`; set it to `none` to write uncompressed buffers.

`object_path_template` controls the relative object path and defaults to the value shown above. It supports these placeholders:

| Placeholder  | Value                                               |
|--------------|-----------------------------------------------------|
| `[signal]`   | Enclosing pdata signal: `logs`, `metrics`, `traces` |
| `[payload]`  | Child payload, e.g. `logs`, `spans`, `span_attrs`   |
| `[pdata_id]` | Pdata UUIDv7 idempotency key                        |
| `[date]`     | UTC date as `YYYY-MM-DD`                            |
| `[year]`     | Four-digit UTC year                                 |
| `[month]`    | Two-digit UTC month                                 |
| `[day]`      | Two-digit UTC day                                   |
| `[hour]`     | Two-digit UTC hour                                  |
| `[minute]`   | Two-digit UTC minute                                |

For example:

```yaml
object_path_template: "otap/v1/signal=[signal]/payload=[payload]/date=[date]/hour=[hour]/[pdata_id].arrow"
```

For a traces `spans` payload, this could produce:

```text
otap/v1/signal=traces/payload=spans/date=2026-07-26/hour=00/019f9bd8-0446-78f0-b8b9-c662301b9876.arrow
```

Templates must be relative and end in `.arrow`. Invalid paths, traversal components, unmatched brackets, and unknown placeholders are rejected during configuration. All placeholders are optional. The operator is responsible for ensuring the template produces distinct paths where required; collisions overwrite existing objects.

## Object Layout

The exporter performs one write for each populated payload in a pdata bundle. With the default template, each write has a distinct path such as:

```text
otap/v1/signal=traces/payload=spans/date=2026-07-26/hour=00/019f9bd8-0446-78f0-b8b9-c662301b9876.arrow
```

The `[signal]` value names the enclosing pdata kind, and `[payload]` uses the stable OTAP `ArrowPayloadType` name. Date and time values come from the pdata UUIDv7 creation time. They are approximate ingestion-time partitioning hints rather than telemetry event timestamps.

Including `[pdata_id]` makes a path stable across retries and durable buffer replay. Its UUIDv7 value makes collisions between workers and load-balanced collector instances impractical. Templates that omit identity placeholders can cause writes to overwrite the same path.

## Commit Semantics

The exporter acknowledges pdata only after every populated payload has been uploaded. Storage failures produce a transient Nack so a retry processor can submit the same pdata UUID and converge on the same object paths.

### Cloud object-store commits

Each payload is published with one object-store Put operation. Retrying a pdata overwrites the same object keys. The exporter does not treat object size as proof that an existing object is complete, and it does not add a writer-defined checksum metadata field.

`ArrowWriter::calculate_payload_sha256` exposes the local SHA-256 digest as raw bytes. A future object-store integration can compare it with a native server checksum and skip a verified object without changing the uploaded encoding or digest calculation.

### Local filesystem commits

Each payload is written to a temporary file beside its destination. By default, the exporter syncs the temporary file, atomically renames it, and syncs the directory hierarchy on platforms that support directory syncing before reporting success. Set `fsync: false` to opt into lower local durability.

## Telemetry

Input pdata message volume is reported by the engine through `channel.receiver.recv.count` and is not duplicated by the exporter.

### Metric Sets

#### `exporter.pdata.exports`

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `exporter.pdata.exports.messages` | `{message}` | `signal`, `outcome` | Number of completed pdata export attempts. `success` means every child object was written; `failure` means conversion or writing failed, including retryable failures. Replayed pdata is counted as another attempt. |

#### `otap.exporter.arrow`

| Metric | Unit | Description |
| --- | --- | --- |
| `otap.exporter.arrow.objects_written` | `{object}` | Number of Arrow IPC objects written by completed pdata exports. |
| `otap.exporter.arrow.bytes_written` | `By` | Number of Arrow IPC bytes written by completed pdata exports. |

### Events

The exporter emits no node-specific telemetry events.

## Reader Compatibility

The exporter preserves native OTAP Arrow schemas, including dictionary-encoded fields. Record-batch buffers use Zstandard compression by default. Arrow IPC readers with dictionary and Zstandard support can read the files and convert them to another analytical format later.

### DataFusion

In `datafusion-cli`, register a payload directory and query its Arrow files directly:

```sql
CREATE EXTERNAL TABLE spans
STORED AS ARROW
LOCATION '/var/lib/otel-arrow/otap/v1/signal=traces/payload=spans/';

-- Replace this sample 16-byte trace ID with the 32 hexadecimal digits to find.
SELECT name, start_time_unix_nano
FROM spans
WHERE trace_id = arrow_cast(
    X'9804accb8087862f83517364af8ed100',
    'FixedSizeBinary(16)'
);
```

### DuckDB

DuckDB's current `nanoarrow` extension does not support dictionary-encoded arrays, so it cannot directly query these OTAP files. This limitation is tracked in [`duckdb-nanoarrow` issue 25](https://github.com/paleolimbot/duckdb-nanoarrow/issues/25).
