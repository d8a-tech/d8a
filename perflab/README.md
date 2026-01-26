# Performance Lab

Local performance testing lab with Alloy, Prometheus, and Jaeger.

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
- **Jaeger UI**: http://localhost:16686 (currently the application does not send traces)
- **Grafana**: http://localhost:3000

## Usage

Launch the server with monitoring enabled and related flags:
```bash
go run main.go server --monitoring-enabled --monitoring-otel-endpoint localhost:4317 --monitoring-otel-export-interval 1s --monitoring-otel-insecure true
```
or just use the bundled development config file:
```bash
go run main.go server --config config.dev.yaml --monitoring-enabled
```

Go to Grafana to see the dashboard: http://localhost:3000

## Production viability

The grafana dashboard and d8a itself with proper `--monitoring-otel-export-interval` should be production viable. All the other configuration here is tuned for performance testing.

