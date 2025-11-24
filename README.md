<img src="./docs/static/img/logo-wide.svg" alt="d8a logo" />

# Divine Data (d8a)

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-d8a-1F7AE0.svg)](https://d8a-tech.github.io/d8a/docs/getting-started)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg)](https://github.com/d8a-tech/d8a/issues)

Divine Data (d8a) is an open source clickstream. It uses GA4 tracking protocol, has clean schema, can be deployed anywhere, including your cloud or on-premises. 

## Highlights

- **GA4 tracking protocol compatibility** - Seamlessly integrates with the Google Analytics 4 tracking protocol, allowing you to implement advanced web, server-side, or mobile tracking plans in just minutes
- **Flat data model for visualization** - Data is stored in a flat, analytics-ready format - perfect as a source for reporting and data warehouses, with even custom events stored in dedicated columns
- **Open source codebase** - Transparent, auditable, and community-driven. Fork, contribute, or self-host as you wish
- **Works alongside GA4** - Can run in parallel with GA4 for a smooth transition or as a reliable backup
- **Full session scope support** - Sessions are calculated on the backend for accuracy and flexibility - no reliance on client-side hacks
- **BigQuery & ClickHouse support** - Export and analyze your data in Google BigQuery or ClickHouse for advanced analytics. Scales to billions of events with no upper limit

## Core Use Cases

- **Perfect for healthcare** - Collect traffic on healthcare websites under HIPAA requirements
- **Well-suited for gov** - Collect traffic on gov't websites under FedRAMP requirements
- **Europe & GDPR?** - Supports EU's independence from Bigtech


## Screenshots


## Getting Started

### Using Docker

Pull and run the latest release:

```bash
docker run --rm -p 8080:8080 \
  -v $(pwd)/config.yaml:/config.yaml \
  ghcr.io/d8a-tech/d8a:latest \
  server --config /config.yaml
```

### From Source

1. Run it.

```bash
go run main.go server
```

2. Make a request and wait a minute for the session to be closed.

```bash
BASE_URL=http://localhost:8080

echo "partition 1, request 1"
CID="ag9"
SESSION_STAMP="127.0.0.1"
curl "${BASE_URL}/g/collect?v=2&tid=G-5T0Z13HKP4&gtm=45je5580h2v9219555710za200&_p=1746817938582&gcd=13l3l3l2l1l1&npa=1&dma_cps=syphamo&dma=1&tag_exp=101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116&cid=${CID}&ul=en-us&sr=1745x982&uaa=x86&uab=64&uafvl=Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171&uamb=0&uam=&uap=Linux&uapv=6.14.5&uaw=0&frm=0&pscdl=noapi&_eu=AAAAAAQ&_s=1&sid=1746817858&sct=1&seg=1&dl=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html&dr=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html&dt=Food%20Shop&en=page_view&_ee=1&tfd=565&sessionStamp=${SESSION_STAMP}&ep.content_group=product&ep.content_id=C_1234" \
  -X 'POST' \
  -H 'authority: region1.google-analytics.com' \
  -H 'accept: */*' \
  -H 'accept-language: en-US,en;q=0.8' \
  -H 'content-length: 0' \
  -H 'origin: https://d8a-tech.github.io' \
  -H 'priority: u=1, i' \
  -H 'referer: https://d8a-tech.github.io/' \
  -H 'sec-ch-ua: "Not(A:Brand";v="24", "Chromium";v="122"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Linux"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: no-cors' \
  -H 'sec-fetch-site: cross-site' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36' ;
```

### Quick volatile ClickHouse single-node via Docker

```bash
docker run --rm -it \
  --name d8a-clickhouse \
  -p 9000:9000 -p 8123:8123 \
  -e CLICKHOUSE_USER=d8a \
  -e CLICKHOUSE_PASSWORD=12345d8a6789 \
  clickhouse/clickhouse-server:latest
```
Reference: [ClickHouse Docker image](https://hub.docker.com/r/clickhouse/clickhouse-server).

Then start d8a with the ClickHouse driver:

```bash
go run main.go server \
  --warehouse clickhouse \
  --clickhouse-host 127.0.0.1 \
  --clickhouse-port 9000 \
  --clickhouse-database default \
  --clickhouse-username d8a \
  --clickhouse-password 12345d8a6789 
```



## Testing

```bash
go test ./...
```

## Contributing


## Documentation


See the following resources:

- [Getting Started Guide](https://d8a-tech.github.io/d8a/docs/getting-started/)
- [Database Schema & Columns](https://d8a-tech.github.io/d8a/docs/columns)
- [Technical Deep Dive](https://d8a-tech.github.io/d8a/docs/deep-dive)

## Join us on Discord

Connect with the team in our Discord community: [link](https://discord.gg/EegbcdsWUc)


## License

MIT — see `LICENSE` for details.

