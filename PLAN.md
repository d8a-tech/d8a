# Plan: Files Warehouse Driver Configuration & Abstraction Design

## Context

**Original request**: Design and configure a new warehouse driver that writes CSV/Parquet files to object storage (S3/GCS). The driver should support local spooling with time-based aggregation before upload, pluggable file formats, and schema-aware file naming. This phase focuses on cmd configuration and abstractions only—no production implementation.

### Key Requirements from Discussion

1. **Single driver with pluggable Format interface** - not separate CSV/Parquet drivers
2. **Separate WAREHOUSE_OBJECT_STORAGE_* configuration** - independent from queue storage
3. **Spool as warehouse.Driver decorator** - writes CSV/Parquet directly to local disk, no double encoding
4. **Uploader with simple interface** - receives file paths, uploads to bucket (not warehouse.Driver)
5. **Buffer tables separately** - each table gets its own local file buffer
6. **Schema hash in filename** - use Arrow's built-in Fingerprint() method
7. **Flag generation utilities** - avoid duplication, maintain accessible flag references
8. **Only abstractions + cmd stubs** - no production code outside pkg/cmd, no test files

### Relevant Codebase Context

**Current warehouse setup**:
- `pkg/warehouse/driver.go`: `Driver` interface with `Write(ctx, table, schema, rows)`, `CreateTable`, `AddColumn`, `MissingColumns`
- `pkg/warehouse/batching.go`: Decorator pattern wrapping drivers with in-memory batching (time-based flush, 1s default)
- `pkg/cmd/warehouse.go`: Factory functions creating BQ/CH drivers wrapped in `NewStaticBatchedDriverRegistry`
- `pkg/cmd/flags.go`: All env vars and CLI flags; current object storage uses `OBJECT_STORAGE_*` prefix
- `pkg/cmd/objectstorage.go`: `createBucket(ctx, cmd) (*blob.Bucket, cleanup, error)` - returns Go CDK blob abstraction

**Object storage**:
- Current flags: `OBJECT_STORAGE_TYPE`, `OBJECT_STORAGE_S3_*`, `OBJECT_STORAGE_GCS_*`
- Config file: Already correctly scoped to `queue.object_storage.*`
- **Must rename env/flags to `QUEUE_OBJECT_STORAGE_*`** to align with config file and make room for warehouse storage

**Patterns observed**:
- Decorator pattern for driver composition (logging, batching)
- Functional options for driver configuration (`WithPartitionBy`, etc.)
- Factory functions in pkg/cmd return `warehouse.Registry` (wraps driver with batching/logging)
- Go CDK `*blob.Bucket` is the universal object storage abstraction
- Flags accessed via `.Name` field (e.g., `cmd.String(bigQueryProjectIDFlag.Name)`)

### Decisions Made

1. **Format is an interface**: `type Format interface { Extension() string; Write(io.Writer, *arrow.Schema, []map[string]any) error; Read(io.Reader) (*arrow.Schema, []map[string]any, error) }`
2. **Spool writes CSV/Parquet directly**: No intermediate format, single encoding pass to final format on local disk
3. **Uploader is NOT warehouse.Driver**: Simple interface `Upload(ctx, filePath) error` - just uploads files to bucket
4. **Schema hashing**: Use Arrow's built-in `schema.Fingerprint()` method (returns string, truncate to 8 chars for filename)
5. **File naming**: `{schema_hash}_{table}_{timestamp}.{ext}` (e.g., `a3b5c7f9_events_2026-02-23T14-00-00Z.csv`)
6. **Warehouse object storage config**: New `WAREHOUSE_OBJECT_STORAGE_*` env vars, `warehouse.object_storage.*` config section
7. **Flag generation with accessible references**: Use struct to hold flag definitions, enable access via `.Name` while generating from common spec

### Assumptions

- Queue and warehouse may use different object storage backends (different providers, buckets, or credentials)
- Spool directory (`--files-spool-dir`) is required configuration (no default)
- Aggregation timing (e.g., 10m local buffer) will be configurable
- Compression handling is embedded in Format implementations (not a separate concern for this phase)
- Each table's spool is independent (separate files, separate flush triggers)
- Single encoding pass (rows → CSV/Parquet on disk) avoids performance overhead

### Explicit Non-Goals

- **No production spool implementation** - only interface definitions and null/stub implementations
- **No CSV/Parquet serialization code** - Format implementations are stubs
- **No test files** - abstractions only, no test coverage in this phase
- **No schema migration handling** - schema changes split files, but no DDL execution
- **No file cleanup/rotation policies** - out of scope for initial design
- **No file merging/compaction** - can be added later as separate background process

---

## Tasks

### Task 1: Rename object storage flags to queue-scoped

**Goal**: Align env var and flag names with config file structure (`queue.object_storage.*`).

**Scope**:
- Rename all env vars in `pkg/cmd/flags.go` from `OBJECT_STORAGE_*` to `QUEUE_OBJECT_STORAGE_*`
- Rename CLI flags from `--object-storage-*` to `--queue-object-storage-*`
- Update flag usage strings to clarify these are for queue backend only
- Update any references in `pkg/cmd/queue.go` that read these flags
- **Do not** change config file keys (already correct: `queue.object_storage.*`)

**Files to modify**:
- `pkg/cmd/flags.go` (lines 357-431): Rename 14 flag definitions
- `pkg/cmd/queue.go` (lines 28-46): Update flag reads using `.Name`
- `pkg/cmd/objectstorage.go` (lines 36-78): Update flag reads if any

**Acceptance criteria**:
- [ ] All `OBJECT_STORAGE_*` env vars renamed to `QUEUE_OBJECT_STORAGE_*`
- [ ] All `--object-storage-*` flags renamed to `--queue-object-storage-*`
- [ ] Config file keys remain unchanged (`queue.object_storage.*`)
- [ ] No compilation errors
- [ ] `go run . run --help` shows renamed flags in correct section

**Implementation progress**: No prior tasks completed.

---

### Task 2: Create flag generation utilities with accessible references

**Goal**: Create reusable utilities to generate object storage flag sets while maintaining accessible flag references.

**Scope**:
- Create `pkg/cmd/flagutils.go` with flag generation utilities
- Define struct to hold flag specifications with accessible `.Name` fields
- Implement function to generate cli.Flag slice from specification
- Enable pattern: `cmd.String(objectStorageFlags.Queue.TypeFlag.Name)`

**Proposed structure**:
```go
// FlagSpec holds a flag definition with accessible Name field
type FlagSpec struct {
    Name        string
    EnvVar      string
    ConfigPath  string
    Usage       string
    DefaultVal  interface{}
    FlagType    string  // "string", "int", "bool", "duration"
}

// ObjectStorageFlagSet holds all object storage flag specs
type ObjectStorageFlagSet struct {
    TypeFlag              FlagSpec
    PrefixFlag            FlagSpec
    S3HostFlag            FlagSpec
    S3PortFlag            FlagSpec
    S3BucketFlag          FlagSpec
    S3AccessKeyFlag       FlagSpec
    S3SecretKeyFlag       FlagSpec
    S3RegionFlag          FlagSpec
    S3ProtocolFlag        FlagSpec
    S3CreateBucketFlag    FlagSpec
    GCSBucketFlag         FlagSpec
    GCSProjectFlag        FlagSpec
    GCSCredsJSONFlag      FlagSpec
}

// ObjectStorageFlags holds queue and warehouse flag sets
type ObjectStorageFlags struct {
    Queue     ObjectStorageFlagSet
    Warehouse ObjectStorageFlagSet
}

// Global accessible flag references
var ObjectStorageFlagsSpec ObjectStorageFlags

// createObjectStorageFlagSet generates flag specs for a given prefix
func createObjectStorageFlagSet(envPrefix, flagPrefix, configPrefix string) ObjectStorageFlagSet

// toCliFlags converts flag specs to []cli.Flag
func toCliFlags(specs ObjectStorageFlagSet) []cli.Flag
```

**Usage pattern**:
```go
// In flags.go initialization:
ObjectStorageFlagsSpec = ObjectStorageFlags{
    Queue: createObjectStorageFlagSet("QUEUE_OBJECT_STORAGE", "queue-object-storage", "queue.object_storage"),
    Warehouse: createObjectStorageFlagSet("WAREHOUSE_OBJECT_STORAGE", "warehouse-object-storage", "warehouse.object_storage"),
}

// In factory functions:
storageType := cmd.String(ObjectStorageFlagsSpec.Queue.TypeFlag.Name)
```

**Files to create**:
- `pkg/cmd/flagutils.go`: Flag generation utilities and structures

**Acceptance criteria**:
- [ ] FlagSpec struct defined with all necessary fields
- [ ] ObjectStorageFlagSet struct holds all 13 object storage flag specs
- [ ] ObjectStorageFlags struct holds Queue and Warehouse flag sets
- [ ] `createObjectStorageFlagSet` generates complete flag set with prefixes
- [ ] `toCliFlags` converts specs to []cli.Flag for urfave/cli
- [ ] Global ObjectStorageFlagsSpec variable enables access via `.Name`
- [ ] No lint errors

**Implementation progress**: Task 1 completed (queue flags renamed, ready to be generated).

---

### Task 3: Refactor queue flags to use generation utilities

**Goal**: Replace hardcoded queue object storage flags with generated ones while maintaining access pattern.

**Scope**:
- In `pkg/cmd/flags.go`, initialize `ObjectStorageFlagsSpec.Queue` flag set
- Replace the 14 manually defined queue object storage flags
- Generate cli.Flag slice using `toCliFlags(ObjectStorageFlagsSpec.Queue)`
- Append to command flags
- Update any hardcoded string references in queue.go to use `ObjectStorageFlagsSpec.Queue.*.Name`

**Implementation approach**:
```go
// In flags.go init() or at package level:
ObjectStorageFlagsSpec = ObjectStorageFlags{
    Queue: createObjectStorageFlagSet(
        "QUEUE_OBJECT_STORAGE",
        "queue-object-storage",
        "queue.object_storage",
    ),
}

var queueObjectStorageCliFlags = toCliFlags(ObjectStorageFlagsSpec.Queue)

// In command:
Flags: append([]cli.Flag{
    // ... other flags
}, queueObjectStorageCliFlags...),
```

**Files to modify**:
- `pkg/cmd/flags.go`: Replace manual flag definitions, add initialization
- `pkg/cmd/queue.go`: Use ObjectStorageFlagsSpec.Queue.*.Name for flag access
- `pkg/cmd/objectstorage.go`: Use ObjectStorageFlagsSpec.Queue.*.Name for flag access

**Acceptance criteria**:
- [ ] Queue object storage flags generated via utility
- [ ] No manual flag definitions for queue storage (lines 357-431 replaced)
- [ ] All flag accesses use ObjectStorageFlagsSpec.Queue.*.Name pattern
- [ ] No hardcoded flag name strings
- [ ] No compilation errors
- [ ] `go run . run --help` shows same flags as before

**Implementation progress**: Task 2 completed (flag generation utilities available).

---

### Task 4: Define files warehouse Format interface and stubs

**Goal**: Create pluggable file format abstraction with CSV and Parquet stub implementations.

**Scope**:
- Create `pkg/warehouse/files/format.go` with Format interface
- Implement stub `csvFormat` and `parquetFormat` (unexported)
- Add constructor functions `NewCSVFormat()` and `NewParquetFormat()` returning `Format`
- Stubs should return `errors.New("not implemented")` for Write/Read operations

**Format interface signature**:
```go
// Format defines how data is serialized to/from files.
type Format interface {
    // Extension returns the file extension (e.g., "csv", "parquet").
    Extension() string
    
    // Write serializes rows to the writer using the provided schema.
    Write(w io.Writer, schema *arrow.Schema, rows []map[string]any) error
    
    // Read deserializes rows from the reader, returning schema and data.
    Read(r io.Reader) (*arrow.Schema, []map[string]any, error)
}
```

**Stub implementations**:
```go
type csvFormat struct{}

func NewCSVFormat() Format {
    return &csvFormat{}
}

func (f *csvFormat) Extension() string {
    return "csv"
}

func (f *csvFormat) Write(w io.Writer, schema *arrow.Schema, rows []map[string]any) error {
    return errors.New("CSV format not implemented")
}

func (f *csvFormat) Read(r io.Reader) (*arrow.Schema, []map[string]any, error) {
    return nil, nil, errors.New("CSV format not implemented")
}
```

**Files to create**:
- `pkg/warehouse/files/format.go`: Interface + stubs

**Acceptance criteria**:
- [ ] Format interface exported with full godoc
- [ ] `NewCSVFormat()` and `NewParquetFormat()` constructors return Format
- [ ] Extension() returns correct strings ("csv", "parquet")
- [ ] Write/Read return "not implemented" errors
- [ ] No lint errors (exported types have comments)

**Implementation progress**: No dependencies on prior tasks.

---

### Task 5: Design schema fingerprinting for file naming

**Goal**: Use Arrow's built-in fingerprinting to generate filename prefixes.

**Scope**:
- Create `pkg/warehouse/files/schema.go` with fingerprinting logic
- Implement `SchemaFingerprint(schema *arrow.Schema) string` - returns 8-char hex prefix using Arrow's Fingerprint()
- Implement `FilenameForWrite(table, fingerprint string, timestamp time.Time, format Format) string`
- Returns format: `{fingerprint}_{table}_{timestamp}.{ext}` (e.g., `a3b5c7f9_events_2026-02-23T14-00-00Z.csv`)

**Fingerprinting logic**:
```go
// SchemaFingerprint returns an 8-character fingerprint for the schema.
// Uses Arrow's built-in Fingerprint() method, truncated to 8 chars.
func SchemaFingerprint(schema *arrow.Schema) string {
    fp := schema.Fingerprint()
    if len(fp) > 8 {
        return fp[:8]
    }
    return fp
}
```

**Timestamp format**: ISO 8601 with hyphens replacing colons for filesystem safety (`2026-02-23T14-00-00Z`)

**Files to create**:
- `pkg/warehouse/files/schema.go`: Fingerprint + filename generation

**Acceptance criteria**:
- [ ] `SchemaFingerprint(*arrow.Schema) string` returns 8-char truncated fingerprint
- [ ] Uses `schema.Fingerprint()` directly (no custom hashing)
- [ ] `FilenameForWrite` generates correct pattern with all components
- [ ] Timestamp formatted safely for filesystems (no colons)
- [ ] No lint errors

**Implementation progress**: Task 4 completed (Format interface available for FilenameForWrite).

---

### Task 6: Define Uploader interface (NOT warehouse.Driver)

**Goal**: Create simple uploader interface for uploading files to object storage.

**Scope**:
- Create `pkg/warehouse/files/uploader.go` with Uploader interface
- Implement stub uploader that logs operations
- Uploader receives file paths, not rows (avoids double encoding)

**Uploader interface**:
```go
// Uploader uploads files to object storage.
type Uploader interface {
    // Upload uploads a file from local disk to object storage.
    // filePath is the local file to upload.
    // Returns error if upload fails.
    Upload(ctx context.Context, filePath string) error
}
```

**Stub implementation**:
```go
type blobUploader struct {
    bucket *blob.Bucket
}

// NewBlobUploader creates an uploader that uploads files to object storage.
func NewBlobUploader(bucket *blob.Bucket) Uploader {
    return &blobUploader{bucket: bucket}
}

// Upload uploads a file to object storage (stub: just logs).
func (u *blobUploader) Upload(ctx context.Context, filePath string) error {
    logrus.WithField("file", filePath).Info("uploader stub: would upload file to bucket")
    return nil
}
```

**Files to create**:
- `pkg/warehouse/files/uploader.go`: Uploader interface + stub

**Acceptance criteria**:
- [ ] Uploader interface defined with Upload(ctx, filePath) method
- [ ] NewBlobUploader(bucket) returns Uploader
- [ ] Upload() logs operation (stub implementation)
- [ ] Interface godoc explains it receives file paths, not rows
- [ ] No lint errors

**Implementation progress**: No dependencies on prior tasks.

---

### Task 7: Define Spool as warehouse.Driver decorator (writes CSV/Parquet directly)

**Goal**: Implement spool that buffers writes to local CSV/Parquet files, then uploads via Uploader.

**Scope**:
- Create `pkg/warehouse/files/spool.go` with spool driver implementation
- Spool implements `warehouse.Driver` interface (decorator pattern)
- Constructor: `NewSpoolDriver(uploader Uploader, format Format, spoolDir string, opts ...SpoolOption) warehouse.Driver`
- Define FlushTrigger interface for pluggable flush strategies
- Implement stub that immediately calls uploader (no actual buffering)
- Buffer each table separately (track per-table state internally)

**Spool driver structure**:
```go
type spoolDriver struct {
    uploader  Uploader          // Uploads files to bucket
    format    Format            // CSV or Parquet
    spoolDir  string
    trigger   FlushTrigger
    
    // Per-table buffering state
    buffers map[string]*tableBuffer
    mu      sync.Mutex
}

type tableBuffer struct {
    file          *os.File        // Open file handle
    writer        io.Writer       // Buffered writer
    currentSchema *arrow.Schema
    fingerprint   string
    rowCount      int
    createdAt     time.Time
}

// FlushTrigger determines when a table's buffer should be flushed.
type FlushTrigger interface {
    // ShouldFlush returns true if buffered data should be uploaded.
    ShouldFlush(rowCount int, age time.Duration) bool
}
```

**Driver interface implementation (stub)**:
```go
func (s *spoolDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
    // Stub: immediately call uploader (no buffering)
    // In real implementation:
    // 1. Get or create table buffer
    // 2. Check if schema changed (compare fingerprints) - if yes, flush and create new file
    // 3. Write rows to local CSV/Parquet file via format.Write()
    // 4. Check if trigger fires (time or size) - if yes, flush
    // 5. Flush: close file, call uploader.Upload(filePath), delete local file
    
    logrus.WithFields(logrus.Fields{
        "table": table,
        "rows":  len(rows),
    }).Info("spool stub: would buffer to local file")
    
    return nil
}

func (s *spoolDriver) CreateTable(table string, schema *arrow.Schema) error {
    return nil // No-op for files
}

func (s *spoolDriver) AddColumn(table string, field *arrow.Field) error {
    return nil // No-op for files
}

func (s *spoolDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
    return nil, nil // Files don't enforce schema
}
```

**Functional options**:
```go
type SpoolOption func(*spoolDriver)

func WithFlushTrigger(trigger FlushTrigger) SpoolOption {
    return func(s *spoolDriver) {
        s.trigger = trigger
    }
}
```

**Files to create**:
- `pkg/warehouse/files/spool.go`: Spool driver + FlushTrigger interface

**Acceptance criteria**:
- [ ] `NewSpoolDriver(uploader, format, spoolDir, ...opts)` returns `warehouse.Driver`
- [ ] Implements all Driver interface methods
- [ ] Write() stub logs operation (no buffering yet)
- [ ] DDL operations are no-ops
- [ ] FlushTrigger interface defined
- [ ] Per-table buffer struct defined (unused in stub)
- [ ] No lint errors

> **Assumption**: In production implementation, Write() would buffer rows to local CSV/Parquet files (via format.Write()), then call uploader.Upload(filePath) when trigger fires. This avoids double encoding - single serialization pass.

**Implementation progress**: Tasks 4-6 completed (Format, schema fingerprinting, and Uploader available).

---

### Task 8: Add warehouse object storage flags using utilities

**Goal**: Generate warehouse object storage flags using the new utilities.

**Scope**:
- In `pkg/cmd/flags.go`, initialize `ObjectStorageFlagsSpec.Warehouse` flag set
- Generate cli.Flag slice using `toCliFlags(ObjectStorageFlagsSpec.Warehouse)`
- Append to command flags

**Implementation approach**:
```go
// Update ObjectStorageFlagsSpec initialization to include Warehouse:
ObjectStorageFlagsSpec = ObjectStorageFlags{
    Queue: createObjectStorageFlagSet(
        "QUEUE_OBJECT_STORAGE",
        "queue-object-storage",
        "queue.object_storage",
    ),
    Warehouse: createObjectStorageFlagSet(
        "WAREHOUSE_OBJECT_STORAGE",
        "warehouse-object-storage",
        "warehouse.object_storage",
    ),
}

var warehouseObjectStorageCliFlags = toCliFlags(ObjectStorageFlagsSpec.Warehouse)

// In command flags:
Flags: append(append([]cli.Flag{
    // ... other flags
}, queueObjectStorageCliFlags...), warehouseObjectStorageCliFlags...),
```

**Files to modify**:
- `pkg/cmd/flags.go`: Add warehouse object storage flag generation

**Acceptance criteria**:
- [ ] Warehouse object storage flags generated via utility
- [ ] All 13 flags have `WAREHOUSE_OBJECT_STORAGE_*` env prefix
- [ ] Flags have `--warehouse-object-storage-*` CLI prefix
- [ ] Config keys map to `warehouse.object_storage.*`
- [ ] No compilation errors
- [ ] `go run . run --help` displays new warehouse flags

**Implementation progress**: Tasks 1-3 completed (flag utilities available and tested with queue flags).

---

### Task 9: Add files warehouse specific flags

**Goal**: Add configuration flags specific to files warehouse driver (format, spool, flush).

**Scope**:
- Add files warehouse flag definitions to `pkg/cmd/flags.go`
- Create flag variables (not hardcoded strings) for later access via `.Name`
- Config keys under `warehouse.files.*` section

**Flags to add**:
```go
var (
    filesFormatFlag = &cli.StringFlag{
        Name:    "files-format",
        EnvVars: []string{"FILES_FORMAT"},
        Usage:   "File format for warehouse output (csv or parquet)",
        Value:   "csv",
        Config:  cli.ConfigPathMap{cli.DefaultConfigName: "warehouse.files.format"},
    }
    
    filesSpoolDirFlag = &cli.StringFlag{
        Name:    "files-spool-dir",
        EnvVars: []string{"FILES_SPOOL_DIR"},
        Usage:   "Local directory for spooling files before upload (required)",
        Config:  cli.ConfigPathMap{cli.DefaultConfigName: "warehouse.files.spool_dir"},
    }
    
    filesFlushIntervalFlag = &cli.DurationFlag{
        Name:    "files-flush-interval",
        EnvVars: []string{"FILES_FLUSH_INTERVAL"},
        Usage:   "Interval for flushing local spool files to object storage",
        Value:   10 * time.Minute,
        Config:  cli.ConfigPathMap{cli.DefaultConfigName: "warehouse.files.flush_interval"},
    }
    
    filesMaxBufferSizeFlag = &cli.IntFlag{
        Name:    "files-max-buffer-size",
        EnvVars: []string{"FILES_MAX_BUFFER_SIZE"},
        Usage:   "Maximum rows to buffer in a file before forcing flush",
        Value:   10000,
        Config:  cli.ConfigPathMap{cli.DefaultConfigName: "warehouse.files.max_buffer_size"},
    }
)
```

**Files to modify**:
- `pkg/cmd/flags.go`: Add 4 new flag variables after warehouse object storage flags

**Acceptance criteria**:
- [ ] All 4 files warehouse flags defined as package variables
- [ ] Config keys use `warehouse.files.*` section
- [ ] Duration and int types properly configured
- [ ] Help text describes each flag clearly
- [ ] Flags accessible via `.Name` field (e.g., `filesFormatFlag.Name`)
- [ ] No compilation errors
- [ ] `go run . run --help` displays new flags under "Files Warehouse Options"

**Implementation progress**: Task 8 completed (warehouse object storage flags added).

---

### Task 10: Add createWarehouseBucket helper function

**Goal**: Extract bucket creation logic for warehouse object storage (parallel to queue's `createBucket`).

**Scope**:
- Create `pkg/cmd/warehouseobjectstorage.go` (new file)
- Implement `createWarehouseBucket(ctx context.Context, cmd *cli.Command) (*blob.Bucket, func() error, error)`
- Mirror the logic in `pkg/cmd/objectstorage.go` but read from warehouse flags using `ObjectStorageFlagsSpec.Warehouse.*.Name`
- Support S3 and GCS providers
- Apply prefix from prefix flag using `blob.PrefixedBucket(bucket, prefix)`

**Function signature**:
```go
// createWarehouseBucket creates an object storage bucket for warehouse file output.
// Returns the bucket, a cleanup function, and any error.
func createWarehouseBucket(ctx context.Context, cmd *cli.Command) (*blob.Bucket, func() error, error)
```

**Implementation approach**:
- Read warehouse object storage flags using `ObjectStorageFlagsSpec.Warehouse.*.Name`
- Example: `storageType := cmd.String(ObjectStorageFlagsSpec.Warehouse.TypeFlag.Name)`
- For S3: create S3 client, open blob.Bucket
- For GCS: create GCS client, open blob.Bucket
- Apply prefix if configured: `blob.PrefixedBucket(bucket, prefix)`
- Return cleanup function that calls `bucket.Close()`

**Files to create**:
- `pkg/cmd/warehouseobjectstorage.go`: New file with createWarehouseBucket function

**Files to reference**:
- `pkg/cmd/objectstorage.go` (lines 25-78): Mirror this logic for warehouse flags

**Acceptance criteria**:
- [ ] `createWarehouseBucket` function returns `(*blob.Bucket, func() error, error)`
- [ ] Supports S3 and GCS providers
- [ ] Reads all warehouse object storage flags via ObjectStorageFlagsSpec.Warehouse.*.Name
- [ ] No hardcoded flag name strings
- [ ] Applies prefix using `blob.PrefixedBucket` if set
- [ ] Returns cleanup function
- [ ] Logs provider selection at info level
- [ ] No lint errors (function has godoc)

**Implementation progress**: Tasks 8-9 completed (all warehouse flags available).

---

### Task 11: Implement createFilesWarehouse factory in pkg/cmd

**Goal**: Add factory function for files warehouse driver in cmd, following BQ/CH pattern.

**Scope**:
- Add `createFilesWarehouse(ctx context.Context, cmd *cli.Command) warehouse.Registry` to `pkg/cmd/warehouse.go`
- Read files warehouse flags using flag variables (e.g., `filesFormatFlag.Name`)
- Create Format based on format flag
- Call `createWarehouseBucket` to get object storage bucket
- Create uploader wrapping bucket
- Create spool driver wrapping uploader
- Wrap in `warehouse.NewStaticBatchedDriverRegistry(ctx, driver)` (consistent with BQ/CH)
- Add "files" case to `warehouseRegistry` switch statement

**Factory function structure**:
```go
func createFilesWarehouse(ctx context.Context, cmd *cli.Command) warehouse.Registry {
    format := cmd.String(filesFormatFlag.Name)
    spoolDir := cmd.String(filesSpoolDirFlag.Name)
    flushInterval := cmd.Duration(filesFlushIntervalFlag.Name)
    maxBufferSize := cmd.Int(filesMaxBufferSizeFlag.Name)
    
    // Validation
    if spoolDir == "" {
        logrus.Fatal("--files-spool-dir is required when using files warehouse")
    }
    
    // Create format
    var fmt whFiles.Format
    switch format {
    case "csv":
        fmt = whFiles.NewCSVFormat()
    case "parquet":
        fmt = whFiles.NewParquetFormat()
    default:
        logrus.Fatalf("unsupported files format: %s", format)
    }
    
    // Create bucket
    bucket, cleanup, err := createWarehouseBucket(ctx, cmd)
    if err != nil {
        logrus.WithError(err).Fatal("failed to create warehouse object storage bucket")
    }
    defer cleanup()  // NOTE: In real impl, store cleanup in run.go
    
    // Create uploader (wraps bucket)
    uploader := whFiles.NewBlobUploader(bucket)
    
    // Create spool (wraps uploader, writes CSV/Parquet directly to disk)
    driver := whFiles.NewSpoolDriver(uploader, fmt, spoolDir)
    
    // Wrap with batching
    return warehouse.NewStaticBatchedDriverRegistry(ctx, driver)
}
```

**Update warehouseRegistry switch**:
```go
case "files":
    return createFilesWarehouse(ctx, cmd)
```

**Files to modify**:
- `pkg/cmd/warehouse.go`: Add createFilesWarehouse function + update switch (after line 235)

**Acceptance criteria**:
- [ ] `createFilesWarehouse` function reads all files warehouse flags via `.Name`
- [ ] No hardcoded flag name strings
- [ ] Validates required flags (spool_dir must be set)
- [ ] Creates Format based on format flag
- [ ] Calls createWarehouseBucket and creates uploader
- [ ] Creates spool driver wrapping uploader
- [ ] Returns batched driver registry
- [ ] "files" case added to warehouseRegistry switch
- [ ] No compilation errors
- [ ] `go run . run --warehouse-driver=files --files-format=csv --files-spool-dir=./tmp/spool --warehouse-object-storage-type=s3 --warehouse-object-storage-s3-bucket=test` starts without errors

**Implementation progress**: All tasks 1-10 completed (all abstractions, flags, and helpers available).

---

### Task 12: Add example config section for files warehouse

**Goal**: Document files warehouse configuration in example config file.

**Scope**:
- Add commented example section to `config.dev.yaml` showing files warehouse configuration
- Include all warehouse object storage options
- Include all files-specific options
- Add usage comments explaining when each option is needed

**Example config structure**:
```yaml
# Warehouse configuration
warehouse:
  driver: files  # Options: bigquery, clickhouse, files, console, noop
  table: events_dev

  # Files warehouse configuration (when driver: files)
  # Writes CSV or Parquet files to object storage (S3 or GCS)
  # Single encoding pass: rows → CSV/Parquet on local disk → upload to bucket
  files:
    format: csv  # Options: csv, parquet
    spool_dir: ./tmp/spool  # Required: local buffer before upload
    flush_interval: 10m  # How often to flush local spool files to object storage
    max_buffer_size: 10000  # Max rows per file before forcing flush
  
  # Object storage for warehouse output (S3 example)
  object_storage:
    type: s3  # Options: s3, gcs
    prefix: d8a/warehouse  # Optional: object key prefix
    s3:
      host: localhost
      port: 9000
      bucket: d8a-warehouse
      access_key: minioadmin
      secret_key: minioadmin
      region: us-east-1
      protocol: http
      create_bucket: true

  # Object storage for warehouse output (GCS example - commented)
  # object_storage:
  #   type: gcs
  #   prefix: d8a/warehouse
  #   gcs:
  #     bucket: d8a-warehouse
  #     project: my-gcp-project
  #     creds_json: '{"type": "service_account", ...}'
```

**Files to modify**:
- `config.dev.yaml`: Add files warehouse section after existing warehouse examples

**Acceptance criteria**:
- [ ] Files warehouse config section added
- [ ] All options documented with comments
- [ ] Both S3 and GCS examples provided (GCS commented out)
- [ ] Usage comments explain purpose of each setting
- [ ] Comment about single encoding pass (no double encoding)
- [ ] Config file remains valid YAML
- [ ] Formatting consistent with existing config

**Implementation progress**: All tasks completed (full configuration system ready).

---

## Architecture Summary

### Data Flow (Avoiding Double Encoding)

```
warehouse.Write(table, schema, rows)
  ↓
spoolDriver.Write(table, schema, rows)
  ↓
[get or create table buffer]
  ↓
format.Write(localFile, schema, rows)  ← CSV/Parquet written ONCE here
  ↓
[check flush trigger: time or size]
  ↓
[if trigger fires]
  ↓
uploader.Upload(filePath)  ← receives file path, not rows
  ↓
bucket.WriteAll(key, fileBytes)
  ↓
os.Remove(filePath)
```

**Key insight**: Single serialization pass (rows → CSV/Parquet on disk), then upload file. No intermediate format, no double encoding.

---

## Open Questions for Implementation Phase

The following design questions were deferred to implementation (user requested stubs only):

1. **Schema change handling**: When schema changes mid-batch, flush immediately and start new file with new schema hash ✅

2. **Flush trigger composition**: OR logic - flush on first trigger (time OR size) ✅

3. **File atomicity**: 
   - Write to temp file in spool dir
   - Close file when complete
   - Upload to bucket
   - Delete local file after successful upload
   - Recommendation: Add `.tmp` suffix during write, rename when complete ✅

4. **Concurrent writes**: 
   - Add mutex to spoolDriver for thread safety
   - Lock per Write() operation (coarse-grained)
   - Per-table buffers protect state ✅

5. **File merging/compaction**: 
   - Out of scope for initial implementation
   - Can be added later as separate background process
   - Would read CSV/Parquet files from bucket, merge, replace ✅

---

## Notes

- **Performance**: Single encoding pass is crucial. Writing CSV/Parquet directly to disk avoids 2x serialization overhead.
- **Simplicity**: Uploader is NOT warehouse.Driver - it's a simpler interface that just uploads files. This clarifies responsibilities.
- **Flag access**: Using struct with .Name fields enables both generation (DRY) and reliable access (type-safe).
- **Arrow fingerprinting**: Built-in method eliminates need for custom hashing logic.
