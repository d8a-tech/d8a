# Files

The files warehouse driver writes session data to either local filesystem or object storage (S3/MinIO or GCS). It does not require a running database.

Data is written continuously to an active file per stream. When a file reaches a size or age threshold it is sealed and uploaded. d8a recovers any undelivered segments automatically on restart.

## What you need

- A local spool directory with sufficient disk space for buffering
- One of: a destination directory (local), an S3/MinIO bucket, or a GCS bucket

## Configuration

:::info Tip
Full configuration reference is available [here](/articles/config#--warehouse-files-format).
:::

Add the following to your `config.yaml` file:

```yaml
storage:
  spool_enabled: true        # required by the files warehouse driver
  spool_directory: ./spool   # where active/sealed segments are staged

warehouse:
  driver: files
  files:
    format: csv
    storage: filesystem      # filesystem, s3, or gcs
    filesystem_path: /data/warehouse
```

## Storage destinations

### Filesystem

Files are moved to the configured directory once sealed. Useful for local pipelines or when another process picks them up from disk.

```yaml
warehouse:
  driver: files
  files:
    storage: filesystem
    filesystem_path: /data/warehouse
```

### S3 / MinIO

```yaml
warehouse:
  driver: files
  files:
    storage: s3

  object_storage:
    s3:
      host: s3.amazonaws.com
      bucket: my-bucket
      access_key: AKIAIOSFODNN7EXAMPLE
      secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      region: us-east-1
      protocol: https
```

### GCS

```yaml
warehouse:
  driver: files
  files:
    storage: gcs

  object_storage:
    gcs:
      bucket: my-gcs-bucket
      creds_json: |
        { "type": "service_account", ... }
```

Leave `creds_json` empty to use Application Default Credentials (ADC).

## Segment tuning

Segments are sealed when either threshold is crossed first.

| Option | Default | Description |
|---|---|---|
| `warehouse.files.max_segment_size` | `1073741824` (1 GiB) | Seal when the active file reaches this size |
| `warehouse.files.max_segment_age` | `1h` | Seal when the active file is this old |
| `warehouse.files.seal_check_interval` | `15s` | How often to evaluate sealing triggers |

## Important notes

- **Spool required**: `storage.spool_enabled` must be `true`. The files warehouse uses the spool directory to stage segments before upload.
- **Schema migrations**: `CreateTable` and `AddColumn` are no-ops. The files warehouse does not create or alter tables. Schema evolution is handled by the consumer of the files.
- **Crash recovery**: On startup d8a scans the spool directory, moves any interrupted uploads back to sealed, and retries them.
- **Upload retries**: A segment that fails to upload is retried up to 3 times. After 3 failures it is moved to a quarantine directory (`streams/<table>/<fingerprint>/failed/`) and will not be retried until manually addressed.

## Verifying your setup

After configuring the files warehouse, start d8a and check the logs. You should see messages indicating segments being sealed and uploaded to your configured destination.
