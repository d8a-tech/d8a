---
sidebar_position: 1
---

# Getting started

Welcome to Divine Data (d8a)! This guide will help you get up and running with d8a, an open source clickstream analytics platform that's fully compatible with GA4 tracking protocols.

Prerequisites:
- Unix shell (Linux, macOS, WSL, etc.)
- Docker installed, with `docker compose` command available
- Basic Unix knowledge (creating files, directories, etc.)

## Step 1: Create configuration file

First create a config file (you can learn more about the configuration options in the [configuration reference](./articles/config)):

```bash
cat > config.yaml <<EOF
storage:
  bolt_directory: /storage/
  queue_directory: /storage/d8a-queue

sessions:
  duration: 10s # Adjust this after the testing phase to production value

warehouse: clickhouse
clickhouse:
  host: clickhouse
  port: "9000"
  database: d8a
  username: default
  password: "verySecuredD8aDatabase"
EOF
```



This particular config configures d8a to use ClickHouse as the warehouse, writes data to `/storage` directory and uses a 10 second session duration.

If you'd like to use a different warehouse, please check the [warehouses](./articles/warehouses) article.   
## Step 2: Create docker compose file

```bash
cat > docker-compose.yml <<EOF
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    restart: unless-stopped
    ports:
      - "8123:8123"  # HTTP interface
      - "9000:9000"  # Native protocol
    volumes:
      - clickhouse-data:/var/lib/clickhouse
    environment:
      - CLICKHOUSE_DB=d8a
      - CLICKHOUSE_USER=default
      - CLICKHOUSE_PASSWORD=verySecuredD8aDatabase
    networks:
      - d8a-network

  d8a:
    image: ghcr.io/d8a-tech/d8a:latest
    container_name: d8a
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - ./config.yaml:/config.yaml:ro
      - d8a-data:/storage
    command: server --config /config.yaml --sessions-duration 10s
    networks:
      - d8a-network
    depends_on:
      - clickhouse

networks:
  d8a-network:
    driver: bridge

volumes:
  d8a-data:
  clickhouse-data:
EOF
```

## Step 3: Start the application

Finally, start the containers:

```bash
docker compose up -d
docker compose logs -f
```

Your d8a instance should be available at `http://localhost:8080`. You may Now send a test tracking request to your d8a instance:

```bash
curl "http://localhost:8080/g/collect?v=2&tid=14&dl=https%3A%2F%2Ffoo.bar&en=page_view&cid=ag9" -X POST
```

D8a server setup is now complete.If you'd like to hook up a domain and use SSL, you need a reverse proxy like Nginx.


## Step 4: Reconfigure Your GA4 Setup

After d8a is up and running, you'll need to reconfigure your current GA4 setup to send traffic to the d8a product. This allows you to capture analytics data using your own infrastructure while maintaining compatibility with GA4 tracking protocols.

Follow the [Intercepting GA4 Events](/guides/intercepting-ga4-events) guide to configure your Google Tag Manager implementation to send data to your d8a endpoint.

The guide covers multiple methods:
- **Method 1 (Recommended)**: Duplicate GA4 requests to keep your existing GA4 setup intact while also sending data to d8a
- **Method 2**: Redirect all GA4 requests to d8a for complete control over your data

Choose the method that best fits your needs based on whether you want to continue sending data to Google Analytics or fully migrate to d8a.

## Next Steps

After completing both steps:
- Verify that events are being received by your d8a instance
- Review the [database schema](/articles/database-schema) to understand the data structure

