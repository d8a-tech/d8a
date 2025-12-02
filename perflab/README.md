# Performance Lab

Local performance testing lab with OpenTelemetry Collector, Prometheus, and Jaeger.

## Setup

Start all services:
```bash
docker compose up -d
```

Stop all services:
```bash
docker compose down
```

## Services

- **OTel Collector**: `localhost:4317` (gRPC), `localhost:4318` (HTTP)
- **Prometheus**: http://localhost:9090
- **Jaeger UI**: http://localhost:16686
- **Grafana**: http://localhost:3000

## Testing

Send test telemetry data:
```bash
go run main.go perflab-test --otel-endpoint localhost:4317 --duration 30s
```

Or with custom intervals:
```bash
go run main.go perflab-test \
  --otel-endpoint localhost:4317 \
  --duration 60s \
  --trace-interval-ms 500 \
  --metric-interval-ms 250
```

## Verification

1. **Traces**: Open http://localhost:16686 and search for service "perflab-test"
2. **Metrics**: Open http://localhost:9090 and query for `perflab_requests` or `perflab_request_duration`


