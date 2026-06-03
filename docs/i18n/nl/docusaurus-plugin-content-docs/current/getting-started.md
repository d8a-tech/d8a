---
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Aan de slag

Welkom bij Divine Data (d8a)! Deze gids helpt je om aan de slag te gaan met d8a, een open-source clickstream-analyticsplatform dat volledig compatibel is met de GA4-trackingprotocollen.

:::caution Cloud-optie beschikbaar
Je kunt beginnen met onze [gratis cloud-optie](https://d8a.tech/pricing/). Bezoek **[de aanmeldpagina](https://app.d8a.tech)** om een account aan te maken. Nadat je je account hebt ingesteld, kun je direct doorgaan naar stap 4.
:::

<Tabs groupId="setup-mode">
  <TabItem value="docker-clickhouse" label="Docker + ClickHouse" default>

  Vereisten:

  * Basiskennis van Unix (bestanden, mappen aanmaken, enzovoort)
  * Unix-shell (Linux, macOS, WSL, enzovoort)
  * Docker geïnstalleerd met het commando `docker compose`
  
  </TabItem>
  <TabItem value="binary-csv" label="Lokale binary + CSV-bestanden">

  Vereisten:
  
  * Basiskennis van Unix (bestanden, mappen aanmaken, enzovoort)
  * Unix-shell (Linux, macOS, WSL, enzovoort)
  * `curl` en `tar` lokaal geïnstalleerd
  
  </TabItem>
</Tabs>

## Stap 1: Maak een configuratiebestand aan

Maak eerst een configuratiebestand aan (meer over de configuratieopties lees je in de [configuratiereferentie](/nl/articles/config)):

<Tabs groupId="setup-mode">
  <TabItem value="docker-clickhouse" label="Docker + ClickHouse" default>

```bash
cat > config.yaml <<EOF
sessions:
  timeout: 10s # Pas dit na de testfase aan naar een productiewaarde

warehouse:
  driver: clickhouse
  clickhouse:
    host: clickhouse
    port: "9000"
    database: d8a
    username: default
    password: "verySecuredD8aDatabase"

protocol: ga4 # Stel in op 'd8a' bij gebruik van de d8a-webtracker en het /d/c-endpoint
EOF
```

  </TabItem>
  <TabItem value="binary-csv" label="Lokale binary + CSV-bestanden">

```bash
cat > config.yaml <<EOF
storage:
  bolt_directory: ./state/bolt
  queue_directory: ./state/queue
  spool_enabled: true
  spool_directory: ./state/spool

currency:
  destination_directory: ./state/currency

sessions:
  timeout: 10s # Pas dit na de testfase aan naar een productiewaarde

warehouse:
  driver: files
  files:
    format: csv
    storage: filesystem
    max_segment_age: 1m
    filesystem:
      path: ./csv-out

protocol: ga4 # Stel in op 'd8a' bij gebruik van de d8a-webtracker en het /d/c-endpoint
EOF
```

  </TabItem>
</Tabs>

:::note
Als je een andere opslag wilt gebruiken, raadpleeg dan het artikel [warehouses](/nl/articles/warehouses).
:::

## Stap 2: Bereid de runtime voor

<Tabs groupId="setup-mode">
  <TabItem value="docker-clickhouse" label="Docker + ClickHouse" default>

```bash
cat > docker-compose.yml <<EOF
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    restart: unless-stopped
    ports:
      - "8123:8123"  # HTTP-interface
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
    command: server --config /config.yaml
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

  </TabItem>
  <TabItem value="binary-csv" label="Lokale binary + CSV-bestanden">

```bash
# Bepaal de nieuwste releaseversie
LATEST_URL=$(curl -fsSL -o /dev/null -w '%{url_effective}' https://github.com/d8a-tech/d8a/releases/latest)
VERSION=${LATEST_URL##*/}

# Detecteer het lokale platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$ARCH" in
  x86_64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

case "$OS" in
  linux|darwin) ;;
  *) echo "Unsupported operating system: $OS"; exit 1 ;;
esac

# Maak de lokale mappen aan
mkdir -p ./state/bolt ./state/queue ./state/spool ./csv-out ./tmp

# Download en pak d8a uit
curl -L "https://github.com/d8a-tech/d8a/releases/download/${VERSION}/d8a_${VERSION#v}_${OS}_${ARCH}.tar.gz" -o ./tmp/d8a.tar.gz
tar -xzf ./tmp/d8a.tar.gz -C ./tmp
chmod +x ./tmp/d8a
```

  </TabItem>
</Tabs>

## Stap 3: Start de applicatie

Start ten slotte d8a:

<Tabs groupId="setup-mode">
  <TabItem value="docker-clickhouse" label="Docker + ClickHouse" default>

```bash
docker compose up -d
docker compose logs -f
```


Je d8a-instantie zou nu beschikbaar moeten zijn op `http://localhost:8080`. Je kunt nu een test-trackingverzoek naar je d8a-instantie sturen:

```bash
curl "http://localhost:8080/g/collect?v=2&tid=14&dl=https%3A%2F%2Ffoo.bar&en=page_view&cid=ag9" -X POST
```

Wacht ongeveer 10 seconden en controleer vervolgens je ClickHouse op de opgeslagen sessie.

  </TabItem>
  <TabItem value="binary-csv" label="Lokale binary + CSV-bestanden">

```bash
./tmp/d8a server --config config.yaml
```


Je d8a-instantie zou nu beschikbaar moeten zijn op `http://localhost:8080`. Je kunt nu een test-trackingverzoek naar je d8a-instantie sturen:

```bash
curl "http://localhost:8080/g/collect?v=2&tid=14&dl=https%3A%2F%2Ffoo.bar&en=page_view&cid=ag9" -X POST
```

Wacht ongeveer twee minuten en controleer vervolgens `./csv-out` op de gegenereerde CSV-bestanden.

  </TabItem>
</Tabs>


Je d8a-serveropstelling is nu compleet. Als je een domein wilt koppelen en SSL wilt gebruiken, heb je een reverse proxy zoals Nginx nodig. Bronnen voor het opzetten van reverse proxy's vind je in de [Nginx-documentatie](https://nginx.org/en/docs/beginners_guide.html) of de [Apache HTTP Server-documentatie](https://httpd.apache.org/docs/2.4/howto/reverse_proxy.html).

## Stap 4: Tracking instellen

Nadat d8a draait, is de volgende stap om trackingverkeer naar je d8a-endpoint te sturen. Zo leg je analyticsgegevens vast op je eigen infrastructuur, terwijl je compatibel blijft met de GA4-trackingprotocollen.

We raden aan te beginnen met [GA4-events onderscheppen](/nl/articles/sources/ga4). Als die aanpak niet bij jouw opstelling past, kun je een andere integratieoptie kiezen uit de sectie **Bronnen** in de zijbalk.

## Volgende stappen

Nadat je alle stappen hebt voltooid:

- Controleer of events worden ontvangen door je d8a-instantie in het warehouse van jouw keuze
- Voor BigQuery kun je het officiële [Data Studio-dashboard](https://datastudio.google.com/u/0/reporting/0e4102b6-c38b-4f55-aa25-c1fe91d1c1e9) kopiëren
- Voor de lokale CSV-optie bekijk je de gegenereerde bestanden in `./csv-out`
- Voor de files-warehousedriver raadpleeg je de [gids voor de files-warehousedriver](/nl/articles/warehouses/files) voor de uploadbestemmingsopties (S3/MinIO, GCS of lokaal bestandssysteem)
- Bekijk het [databaseschema](/nl/articles/database-schema) om de datastructuur te begrijpen
