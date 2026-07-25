// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Uploads OTAP pdata payloads as standalone Arrow IPC files.
//!
//! Each object is published atomically. Object names are deterministic from the
//! pdata UUIDv7, signal, payload type, and configured path template. Existing
//! objects are uploaded again.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::path::{Path as FilePath, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use otap_df_otap::object_store::StorageType;
use otap_df_pdata::otap::OtapArrowRecords;
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::config::{Compression, Config, ObjectPathTemplate};

/// Default maximum number of payload uploads in flight for one pdata batch.
const DEFAULT_MAX_CONCURRENT_UPLOADS: usize = 8;

/// Result statistics for a completed pdata upload.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Number of Arrow IPC payload objects written.
    pub objects_written: u64,
    /// Total Arrow IPC bytes written.
    pub bytes_written: u64,
}

/// Failure returned while preparing or uploading pdata.
#[derive(Debug, Error)]
pub enum ArrowWriterError {
    /// The configured object store could not be constructed.
    #[error("failed to configure Arrow IPC object storage: {source}")]
    ConfigureStorage {
        /// Underlying object-store configuration error.
        #[source]
        source: object_store::Error,
    },

    /// A pdata idempotency key is not a usable UUIDv7.
    #[error("pdata idempotency key {idempotency_key} has no valid UUIDv7 timestamp")]
    InvalidIdempotencyKey {
        /// Invalid pdata idempotency key.
        idempotency_key: Uuid,
    },

    /// A rendered object path is invalid.
    #[error("object path template produced invalid path {path}: {source}")]
    InvalidObjectPath {
        /// Rendered object path.
        path: String,
        /// Object-store path validation error.
        #[source]
        source: object_store::path::Error,
    },

    /// An OTAP pdata payload could not be encoded as Arrow IPC.
    #[error("failed to encode pdata payload {payload_type:?} as Arrow IPC: {source}")]
    Encode {
        /// OTAP payload type that could not be encoded.
        payload_type: ArrowPayloadType,
        /// Underlying Arrow encoding error.
        #[source]
        source: arrow::error::ArrowError,
    },

    /// An atomic object upload failed.
    #[error("failed to upload Arrow IPC object {location}: {source}")]
    Upload {
        /// Object-store location that failed.
        location: Path,
        /// Underlying object-store error.
        #[source]
        source: object_store::Error,
    },

    /// A local Arrow IPC object could not be written or synced.
    #[error("failed to write local Arrow IPC object {path}: {source}")]
    LocalUpload {
        /// Local destination path that failed.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },

    /// The blocking local upload task failed.
    #[error("local Arrow IPC upload task failed for {location}: {source}")]
    LocalUploadTask {
        /// Object-store location being written.
        location: Path,
        /// Tokio blocking-task failure.
        #[source]
        source: tokio::task::JoinError,
    },
}

enum WriteTarget {
    ObjectStore(Arc<dyn ObjectStore>),
    Local(LocalWriteTarget),
}

struct LocalWriteTarget {
    file_system: LocalFileSystem,
    root: PathBuf,
    fsync: bool,
}

/// Writes OTAP pdata payloads to standalone Arrow IPC objects.
pub struct ArrowWriter {
    target: WriteTarget,
    max_concurrent_uploads: NonZeroUsize,
    compression: Compression,
    object_path_template: ObjectPathTemplate,
}

impl ArrowWriter {
    /// Creates a writer using bounded concurrent payload uploads.
    #[must_use]
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            target: WriteTarget::ObjectStore(object_store),
            max_concurrent_uploads: NonZeroUsize::new(DEFAULT_MAX_CONCURRENT_UPLOADS)
                .expect("default upload concurrency is non-zero"),
            compression: Compression::default(),
            object_path_template: ObjectPathTemplate::default(),
        }
    }

    /// Creates a writer for the configured local or cloud storage backend.
    ///
    /// Configured local storage uses atomic staged files and honors
    /// [`Config::fsync`]. Other storage backends use their object-store `PUT`
    /// implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured storage backend cannot be created.
    pub fn from_config(config: &Config) -> Result<Self, ArrowWriterError> {
        if let Some(retry) = &config.retry {
            retry
                .validate()
                .map_err(|source| ArrowWriterError::ConfigureStorage { source })?;
        }

        // Cloud variants are enabled on StorageType by top-level Cargo features.
        #[allow(unreachable_patterns)]
        let target = match &config.storage {
            StorageType::File { base_uri } => {
                let file_system = LocalFileSystem::new_with_prefix(base_uri)
                    .map_err(|source| ArrowWriterError::ConfigureStorage { source })?;
                let root = std::fs::canonicalize(base_uri).map_err(|source| {
                    ArrowWriterError::LocalUpload {
                        path: PathBuf::from(base_uri),
                        source,
                    }
                })?;
                WriteTarget::Local(LocalWriteTarget {
                    file_system,
                    root,
                    fsync: config.fsync,
                })
            }
            _ => {
                let object_store = otap_df_otap::object_store::from_storage_type_with_retry(
                    &config.storage,
                    config.retry.as_ref(),
                )
                .map_err(|source| ArrowWriterError::ConfigureStorage { source })?;
                WriteTarget::ObjectStore(object_store)
            }
        };

        Ok(Self {
            target,
            max_concurrent_uploads: NonZeroUsize::new(DEFAULT_MAX_CONCURRENT_UPLOADS)
                .expect("default upload concurrency is non-zero"),
            compression: config.compression,
            object_path_template: config.object_path_template.clone(),
        })
    }

    /// Overrides the maximum number of payload uploads in flight per pdata batch.
    #[must_use]
    pub const fn with_max_concurrent_uploads(
        mut self,
        max_concurrent_uploads: NonZeroUsize,
    ) -> Self {
        self.max_concurrent_uploads = max_concurrent_uploads;
        self
    }

    /// Overrides the object path template.
    #[must_use]
    pub fn with_object_path_template(mut self, object_path_template: ObjectPathTemplate) -> Self {
        self.object_path_template = object_path_template;
        self
    }

    /// Overrides Arrow IPC record-batch buffer compression.
    #[must_use]
    pub const fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Calculates SHA-256 over a payload's standalone Arrow IPC encoding.
    ///
    /// The raw digest can be encoded as required by a future object-store
    /// native checksum API. The digest covers exactly the bytes uploaded by
    /// [`write_pdata`](Self::write_pdata).
    ///
    /// # Errors
    ///
    /// Returns an error if Arrow IPC encoding fails.
    pub fn calculate_payload_sha256(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<[u8; 32], arrow::error::ArrowError> {
        encode_payload(batch, self.compression).map(|bytes| calculate_sha256(&bytes))
    }

    /// Uploads every populated Arrow payload in one pdata batch.
    ///
    /// Each object key has this form:
    ///
    /// ```text
    /// otap/v1/signal=<signal>/payload=<payload>/date=<YYYY-MM-DD>/hour=<HH>/
    ///   <uuidv7>.arrow
    /// ```
    ///
    /// The date and hour are derived from the pdata UUIDv7 creation timestamp.
    /// They are intended for partition pruning, not as telemetry event
    /// timestamps.
    ///
    /// Each individual object is published atomically. Existing objects are
    /// uploaded again using an atomic overwrite.
    ///
    /// # Errors
    ///
    /// Returns an error if the pdata key is not UUIDv7, a payload cannot be
    /// encoded, or an upload fails.
    pub async fn write_pdata(
        &self,
        idempotency_key: Uuid,
        records: &OtapArrowRecords,
    ) -> Result<WriteStats, ArrowWriterError> {
        let objects = prepare_objects(
            idempotency_key,
            records,
            &self.object_path_template,
            self.compression,
        )?;
        let target = &self.target;

        stream::iter(objects)
            .map(|(location, payload)| async move {
                let byte_count = payload.len() as u64;
                target.put(&location, payload).await?;

                Ok(byte_count)
            })
            .buffer_unordered(self.max_concurrent_uploads.get())
            .try_fold(WriteStats::default(), |mut stats, byte_count| async move {
                stats.objects_written += 1;
                stats.bytes_written += byte_count;
                Ok(stats)
            })
            .await
    }
}

impl WriteTarget {
    async fn put(&self, location: &Path, payload: Bytes) -> Result<(), ArrowWriterError> {
        match self {
            Self::ObjectStore(object_store) => {
                let _ = object_store
                    .put(location, payload.into())
                    .await
                    .map_err(|source| ArrowWriterError::Upload {
                        location: location.clone(),
                        source,
                    })?;
                Ok(())
            }
            Self::Local(target) => target.put(location, payload).await,
        }
    }
}

impl LocalWriteTarget {
    async fn put(&self, location: &Path, payload: Bytes) -> Result<(), ArrowWriterError> {
        let destination = self
            .file_system
            .path_to_filesystem(location)
            .map_err(|source| ArrowWriterError::Upload {
                location: location.clone(),
                source,
            })?;
        let root = self.root.clone();
        let fsync = self.fsync;
        let task_location = location.clone();
        let error_path = destination.clone();

        tokio::task::spawn_blocking(move || {
            write_local_object(&root, &destination, &payload, fsync)
        })
        .await
        .map_err(|source| ArrowWriterError::LocalUploadTask {
            location: task_location,
            source,
        })?
        .map_err(|source| ArrowWriterError::LocalUpload {
            path: error_path,
            source,
        })
    }
}

struct StagedFile {
    file: Option<File>,
    path: PathBuf,
    installed: bool,
}

impl StagedFile {
    fn create(destination: &FilePath) -> io::Result<Self> {
        loop {
            let path = staging_path(destination);
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok(Self {
                        file: Some(file),
                        path,
                        installed: false,
                    });
                }
                Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
                Err(source) => return Err(source),
            }
        }
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if !self.installed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn write_local_object(
    root: &FilePath,
    destination: &FilePath,
    payload: &[u8],
    fsync: bool,
) -> io::Result<()> {
    let parent = destination.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("local destination has no parent: {}", destination.display()),
        )
    })?;
    std::fs::create_dir_all(parent)?;

    let mut staged = StagedFile::create(destination)?;
    let file = staged
        .file
        .as_mut()
        .expect("new staged upload contains an open file");
    file.write_all(payload)?;
    if fsync {
        file.sync_all()?;
    }
    drop(staged.file.take());
    std::fs::rename(&staged.path, destination)?;
    staged.installed = true;

    if fsync {
        sync_directory_hierarchy(root, parent)?;
    }
    Ok(())
}

fn staging_path(destination: &FilePath) -> PathBuf {
    let file_name = destination
        .file_name()
        .expect("validated object-store paths have a file name")
        .to_string_lossy();
    destination.with_file_name(format!(".{file_name}.{}.tmp", Uuid::new_v4()))
}

#[cfg(unix)]
fn sync_directory_hierarchy(root: &FilePath, leaf: &FilePath) -> io::Result<()> {
    let relative = leaf.strip_prefix(root).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "local destination {} is outside configured root {}",
                leaf.display(),
                root.display()
            ),
        )
    })?;
    let mut directory = root.to_path_buf();
    File::open(&directory)?.sync_all()?;
    for component in relative.components() {
        directory.push(component);
        File::open(&directory)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory_hierarchy(_root: &FilePath, _leaf: &FilePath) -> io::Result<()> {
    Ok(())
}

fn calculate_sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn prepare_objects(
    idempotency_key: Uuid,
    records: &OtapArrowRecords,
    object_path_template: &ObjectPathTemplate,
    compression: Compression,
) -> Result<Vec<(Path, Bytes)>, ArrowWriterError> {
    let mut objects = Vec::new();
    let timestamp = pdata_timestamp(idempotency_key)?;
    let signal = signal_name(records);

    for &payload_type in records.allowed_payload_types() {
        let Some(batch) = records.get(payload_type) else {
            continue;
        };
        let encoded =
            encode_payload(batch, compression).map_err(|source| ArrowWriterError::Encode {
                payload_type,
                source,
            })?;
        let location = payload_location(
            object_path_template,
            idempotency_key,
            signal,
            payload_type,
            timestamp,
        )?;
        objects.push((location, Bytes::from(encoded)));
    }

    Ok(objects)
}

fn encode_payload(
    batch: &arrow::record_batch::RecordBatch,
    compression: Compression,
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut encoded = Vec::new();
    {
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(compression.ipc_type())?;
        let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
            &mut encoded,
            &batch.schema(),
            options,
        )?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(encoded)
}

fn pdata_timestamp(idempotency_key: Uuid) -> Result<DateTime<Utc>, ArrowWriterError> {
    if idempotency_key.get_version_num() != 7 {
        return Err(ArrowWriterError::InvalidIdempotencyKey { idempotency_key });
    }
    let timestamp = idempotency_key
        .get_timestamp()
        .ok_or(ArrowWriterError::InvalidIdempotencyKey { idempotency_key })?;
    let (seconds, nanos) = timestamp.to_unix();
    let seconds = i64::try_from(seconds)
        .map_err(|_| ArrowWriterError::InvalidIdempotencyKey { idempotency_key })?;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .ok_or(ArrowWriterError::InvalidIdempotencyKey { idempotency_key })
}

const fn signal_name(records: &OtapArrowRecords) -> &'static str {
    match records {
        OtapArrowRecords::Logs(_) => "logs",
        OtapArrowRecords::Metrics(_) => "metrics",
        OtapArrowRecords::Traces(_) => "traces",
    }
}

fn payload_location(
    object_path_template: &ObjectPathTemplate,
    idempotency_key: Uuid,
    signal: &str,
    payload_type: ArrowPayloadType,
    timestamp: DateTime<Utc>,
) -> Result<Path, ArrowWriterError> {
    let payload = payload_type.as_str_name().to_ascii_lowercase();
    let rendered = object_path_template
        .as_str()
        .replace("[signal]", signal)
        .replace("[payload]", &payload)
        .replace("[pdata_id]", &idempotency_key.to_string())
        .replace("[date]", &timestamp.format("%Y-%m-%d").to_string())
        .replace("[year]", &format!("{:04}", timestamp.year()))
        .replace("[month]", &format!("{:02}", timestamp.month()))
        .replace("[day]", &format!("{:02}", timestamp.day()))
        .replace("[hour]", &format!("{:02}", timestamp.hour()))
        .replace("[minute]", &format!("{:02}", timestamp.minute()));

    Path::parse(&rendered).map_err(|source| ArrowWriterError::InvalidObjectPath {
        path: rendered,
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow_ipc::reader::FileReader;
    use async_trait::async_trait;
    use futures::{TryStreamExt, stream::BoxStream};
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    };
    use otap_df_pdata::otap::OtapBatchStore;
    use otap_df_pdata::otap::testing::logs;
    use otap_df_pdata::record_batch;

    use super::*;

    #[derive(Debug)]
    struct FailOnceStore {
        inner: Arc<InMemory>,
        failed: AtomicBool,
        path_fragment: &'static str,
    }

    impl FailOnceStore {
        fn new(inner: Arc<InMemory>, path_fragment: &'static str) -> Self {
            Self {
                inner,
                failed: AtomicBool::new(false),
                path_fragment,
            }
        }
    }

    impl fmt::Display for FailOnceStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("fail-once object store")
        }
    }

    #[async_trait]
    impl ObjectStore for FailOnceStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            if location.as_ref().contains(self.path_fragment)
                && !self.failed.swap(true, Ordering::SeqCst)
            {
                return Err(object_store::Error::Generic {
                    store: "fail-once",
                    source: Box::new(io::Error::other("injected upload failure")),
                });
            }

            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn test_records() -> OtapArrowRecords {
        OtapArrowRecords::Logs(logs!(
            (Logs, ("id", UInt16, vec![0u16, 1])),
            (LogAttrs, ("parent_id", UInt16, vec![0u16, 1])),
            (ResourceAttrs, ("parent_id", UInt16, vec![0u16, 1])),
            (ScopeAttrs, ("parent_id", UInt16, vec![0u16, 1]))
        ))
    }

    fn expected_objects(idempotency_key: Uuid, records: &OtapArrowRecords) -> Vec<(Path, Bytes)> {
        prepare_objects(
            idempotency_key,
            records,
            &ObjectPathTemplate::default(),
            Compression::default(),
        )
        .expect("prepare expected objects")
    }

    /// Scenario: An OTAP pdata payload is encoded as a standalone Arrow IPC file.
    /// Guarantees: The public checksum method covers exactly the bytes prepared for upload.
    #[test]
    fn calculates_sha256_for_each_payload() {
        let records = test_records();
        let writer = ArrowWriter::new(Arc::new(InMemory::new()));

        for &payload_type in records.allowed_payload_types() {
            let Some(batch) = records.get(payload_type) else {
                continue;
            };
            let encoded =
                encode_payload(batch, Compression::Zstd).expect("encode compressed payload");
            let expected = calculate_sha256(&encoded);
            let actual = writer
                .calculate_payload_sha256(batch)
                .expect("calculate payload SHA-256");

            assert_eq!(actual, expected);
        }
    }

    /// Scenario: The same repetitive Arrow batch is encoded with default and disabled compression.
    /// Guarantees: Zstandard is readable and reduces bytes while `none` remains a valid opt-out.
    #[test]
    fn supports_default_zstd_and_optional_uncompressed_ipc() {
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![7u16; 16_384]))));
        let batch = records.get(ArrowPayloadType::Logs).expect("logs payload");
        let compressed =
            encode_payload(batch, Compression::default()).expect("encode compressed payload");
        let uncompressed =
            encode_payload(batch, Compression::None).expect("encode uncompressed payload");

        assert!(compressed.len() < uncompressed.len());
        for encoded in [compressed, uncompressed] {
            let mut reader =
                FileReader::try_new(Cursor::new(encoded), None).expect("open Arrow IPC file");
            assert_eq!(
                reader.next().expect("one batch").expect("decode batch"),
                *batch
            );
            assert!(reader.next().is_none());
        }
    }

    /// Scenario: One pdata batch contains several OTAP Arrow payloads.
    /// Guarantees: Every populated payload becomes a deterministic standalone Arrow IPC object.
    #[tokio::test]
    async fn writes_each_pdata_payload_as_an_arrow_file() {
        let idempotency_key = Uuid::now_v7();
        let records = test_records();
        let expected = expected_objects(idempotency_key, &records);
        let object_store = Arc::new(InMemory::new());
        let writer = ArrowWriter::new(object_store.clone());

        let stats = writer
            .write_pdata(idempotency_key, &records)
            .await
            .expect("write pdata payloads");
        let objects = object_store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list objects");

        assert_eq!(expected.len(), 4);
        assert_eq!(stats.objects_written, expected.len() as u64);
        assert_eq!(
            stats.bytes_written,
            expected
                .iter()
                .map(|(_, payload)| payload.len() as u64)
                .sum::<u64>()
        );
        assert_eq!(objects.len(), expected.len());

        for (location, expected_bytes) in expected {
            let bytes = object_store
                .get(&location)
                .await
                .expect("get uploaded payload")
                .bytes()
                .await
                .expect("read uploaded payload");

            assert_eq!(bytes, expected_bytes);
            assert!(location.as_ref().contains("/signal=logs/"));
            assert!(
                location
                    .as_ref()
                    .ends_with(&format!("{idempotency_key}.arrow"))
            );

            let reader =
                FileReader::try_new(Cursor::new(bytes), None).expect("open uploaded Arrow file");
            assert_eq!(reader.count(), 1);
        }
    }

    /// Scenario: Pdata contains a payload using the production OTAP logs schema.
    /// Guarantees: Exported Arrow IPC decodes to the same OTAP record batch and pdata path.
    #[tokio::test]
    async fn writes_actual_otap_logs_payload() {
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![0u16, 1]))));
        let source = records
            .get(ArrowPayloadType::Logs)
            .expect("logs payload")
            .clone();
        let idempotency_key = Uuid::now_v7();
        let object_store = Arc::new(InMemory::new());
        let writer = ArrowWriter::new(object_store.clone());

        let stats = writer
            .write_pdata(idempotency_key, &records)
            .await
            .expect("write OTAP logs payload");
        let objects = object_store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list objects");

        assert_eq!(stats.objects_written, 1);
        assert_eq!(objects.len(), 1);
        assert!(objects[0].location.as_ref().contains("payload=logs/"));
        assert!(
            objects[0]
                .location
                .as_ref()
                .ends_with(&format!("{idempotency_key}.arrow"))
        );

        let bytes = object_store
            .get(&objects[0].location)
            .await
            .expect("get OTAP logs object")
            .bytes()
            .await
            .expect("read OTAP logs object");
        let mut reader =
            FileReader::try_new(Cursor::new(bytes), None).expect("open OTAP logs Arrow file");
        let decoded = reader
            .next()
            .expect("one Arrow batch")
            .expect("decode OTAP logs batch");

        assert_eq!(decoded, source);
        assert!(reader.next().is_none());
    }

    /// Scenario: One pdata batch is written twice using default configured local storage.
    /// Guarantees: Durable local files remain valid Arrow IPC and retries replace every payload.
    #[tokio::test]
    async fn writes_pdata_objects_to_the_local_filesystem() {
        let idempotency_key = Uuid::now_v7();
        let records = test_records();
        let expected = expected_objects(idempotency_key, &records);
        let output_directory = tempfile::tempdir().expect("create output directory");
        let config = Config {
            storage: StorageType::File {
                base_uri: output_directory.path().to_string_lossy().into_owned(),
            },
            retry: None,
            fsync: true,
            compression: Compression::default(),
            object_path_template: ObjectPathTemplate::default(),
        };
        let writer = ArrowWriter::from_config(&config).expect("create configured writer");

        let stats = writer
            .write_pdata(idempotency_key, &records)
            .await
            .expect("write pdata payloads");
        let retry_stats = writer
            .write_pdata(idempotency_key, &records)
            .await
            .expect("rewrite pdata payloads");

        assert_eq!(stats.objects_written, expected.len() as u64);
        assert_eq!(retry_stats.objects_written, expected.len() as u64);
        for (location, expected_bytes) in expected {
            let path = output_directory.path().join(location.as_ref());
            let bytes = std::fs::read(&path).expect("read Arrow file directly");

            assert_eq!(bytes.as_slice(), expected_bytes.as_ref());
            let reader =
                FileReader::try_new(Cursor::new(bytes), None).expect("open local Arrow file");
            assert_eq!(reader.count(), 1);
        }
    }

    /// Scenario: One payload upload fails after a pdata write has started.
    /// Guarantees: Retrying converges to one complete object per deterministic pdata path.
    #[tokio::test]
    async fn retry_after_partial_failure_completes_the_same_pdata_objects() {
        let idempotency_key = Uuid::now_v7();
        let records = test_records();
        let expected_count = expected_objects(idempotency_key, &records).len();
        let object_store = Arc::new(InMemory::new());
        let failing_store = Arc::new(FailOnceStore::new(
            object_store.clone(),
            "payload=log_attrs",
        ));
        let writer = ArrowWriter::new(failing_store);

        let first_result = writer.write_pdata(idempotency_key, &records).await;
        assert!(matches!(first_result, Err(ArrowWriterError::Upload { .. })));
        let retry_stats = writer
            .write_pdata(idempotency_key, &records)
            .await
            .expect("retry write");
        let objects = object_store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list objects");

        assert_eq!(objects.len(), expected_count);
        assert_eq!(retry_stats.objects_written, expected_count as u64);
    }

    /// Scenario: A caller supplies an identity that is not UUIDv7.
    /// Guarantees: The writer refuses to publish objects without a time-sortable pdata identity.
    #[tokio::test]
    async fn rejects_non_uuidv7_idempotency_keys() {
        let object_store = Arc::new(InMemory::new());
        let writer = ArrowWriter::new(object_store.clone());

        let result = writer.write_pdata(Uuid::new_v4(), &test_records()).await;

        assert!(matches!(
            result,
            Err(ArrowWriterError::InvalidIdempotencyKey { .. })
        ));
        assert!(
            object_store
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .expect("list objects")
                .is_empty()
        );
    }

    /// Scenario: A path template uses every supported partition placeholder.
    /// Guarantees: Values are rendered from the pdata UUIDv7 timestamp in UTC.
    #[tokio::test]
    async fn renders_configured_object_path_template() {
        let idempotency_key =
            Uuid::parse_str("019f9bd8-0446-78f0-b8b9-c662301b9876").expect("parse UUIDv7");
        let object_store = Arc::new(InMemory::new());
        let template = ObjectPathTemplate::parse(
            "[year]-[month]-[day]/hour=[hour]/minute=[minute]/signal=[signal]/\
             payload=[payload]/[pdata_id].arrow",
        )
        .expect("parse path template");
        let writer = ArrowWriter::new(object_store.clone()).with_object_path_template(template);
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![0u16]))));

        let _ = writer
            .write_pdata(idempotency_key, &records)
            .await
            .expect("write pdata");
        let objects = object_store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list objects");

        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0].location.as_ref(),
            "2026-07-26/hour=00/minute=34/signal=logs/payload=logs/\
             019f9bd8-0446-78f0-b8b9-c662301b9876.arrow"
        );
    }
}
