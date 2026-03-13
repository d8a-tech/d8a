# Production operations

## Deployment options

Today, d8a documents two practical deployment shapes:

- **Single-node Docker Compose**: the simplest way to run d8a with ClickHouse on one machine. See the [Getting started guide](/getting-started).
- **Split processes on your own infrastructure**: run `d8a receiver` and `d8a worker` separately, which is the recommended base for more production-oriented setups.

If you build and maintain other deployment options such as Helm charts, Kubernetes manifests, or Terraform-based examples, contributions are very welcome.

## HA

D8A supports running the HTTP receiver and the background worker separately, allowing HA setups.

Constraints:

- Multiple receivers are supported.
- Only a single worker is supported (session state lives in the worker's local BoltDB). Horizontal scaling of workers is not possible, so you're left with vertical scaling, but it shouldn't be a big problem—a machine with 16 cores and 30GB of RAM should handle traffic around 7K reqps, which translates to around 7B events/month. For ~100M traffic you should be good with 2 CPUs and 4GB of RAM.

### Modes

- `d8a server`: receiver + worker in one process (default).
- `d8a receiver`: receiver only (HTTP server; publishes to queue).
- `d8a worker`: worker only (consumes from queue; no HTTP server).

### Queue backends

Two queue backends exist for the receiver/worker boundary:

- `filesystem` (default): a local directory queue.
- `objectstorage`: a shared object-storage-backed queue (Go CDK `blob.Bucket`).

When running multiple receivers on different nodes, use `objectstorage`.

### Example: MinIO (S3-compatible)

YAML (`config.yaml`):

```yaml
queue:
  backend: object_storage
  
  object_storage:
    prefix: d8a/dev/queue
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

- Use `queue.object_storage.prefix` to namespace environments (prevents cross-talk within a shared bucket).
- The system is at-least-once: tasks can be replayed if the worker crashes after processing but before deletion.
