# HA mode (receiver/worker split)

OSS `d8a` supports running the HTTP receiver and the background worker separately.

Constraints in OSS:

- Multiple receivers are supported.
- Only a single worker is supported (session state lives in the worker's local BoltDB).

## Modes

- `d8a server`: receiver + worker in one process (default).
- `d8a receiver`: receiver only (HTTP server; publishes to queue).
- `d8a worker`: worker only (consumes from queue; no HTTP server).

## Queue backends

Two queue backends exist for the receiver/worker boundary:

- `filesystem` (default): a local directory queue.
- `objectstorage`: a shared object-storage-backed queue (Go CDK `blob.Bucket`).

When running multiple receivers on different nodes, use `objectstorage`.

## Example: MinIO (S3-compatible)

YAML (`config.yaml`):

```yaml
queue:
  backend: objectstorage
  object_prefix: d8a/dev/queue

object_storage:
  type: s3
  s3:
    host: 127.0.0.1
    port: 9000
    protocol: http
    bucket: d8a-queue
    access_key: minioadmin
    secret_key: minioadmin
    region: us-east-1
    create_bucket: true
```

Run receiver(s):

```bash
go run . receiver --config config.yaml --server-port 8080
```

Run the worker (single instance):

```bash
go run . worker --config config.yaml --storage-bolt-directory ./state
```

Notes:

- Use `queue.object_prefix` to namespace environments (prevents cross-talk within a shared bucket).
- The system is at-least-once: tasks can be replayed if the worker crashes after processing but before deletion.
