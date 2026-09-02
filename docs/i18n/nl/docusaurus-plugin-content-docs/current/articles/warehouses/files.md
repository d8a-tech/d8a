# Bestanden

De files-warehousedriver schrijft session-data naar het lokale bestandssysteem of naar object storage (S3/MinIO of GCS). Hij vereist geen draaiende database.

Data wordt continu naar een actief bestand per stream geschreven. Wanneer een bestand een grootte- of leeftijdsdrempel bereikt, wordt het verzegeld en geüpload. d8a herstelt bij het opnieuw starten automatisch alle nog niet afgeleverde segmenten.

## Wat je nodig hebt

- Een lokale spool-directory met voldoende schijfruimte voor buffering
- Een van: een bestemmingsdirectory (lokaal), een S3/MinIO-bucket of een GCS-bucket

## Configuratie

:::info Tip
De volledige configuratiereferentie is [hier](/nl/articles/config#--warehouse-files-format) beschikbaar.
:::

Voeg het volgende toe aan je `config.yaml`-bestand:

```yaml
storage:
  spool_enabled: true        # required by the files warehouse driver
  spool_directory: ./spool   # where active/sealed segments are staged

warehouse:
  driver: files
  files:
    format: csv
    storage: filesystem      # filesystem, s3, or gcs
    filesystem:
      path: /data/warehouse
```

## Opslagbestemmingen

### Filesystem

Bestanden worden naar de geconfigureerde directory verplaatst zodra ze verzegeld zijn. Handig voor lokale pipelines of wanneer een ander proces ze van schijf oppikt.

```yaml
warehouse:
  driver: files
  files:
    storage: filesystem
    filesystem:
      path: /data/warehouse
```

### S3 / MinIO

```yaml
warehouse:
  driver: files
  files:
    storage: s3
    s3:
      host: s3.amazonaws.com
      bucket: my-bucket
      access_key: AKIAIOSFODNN7EXAMPLE
      secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      region: us-east-1
      protocol: https
      disable_upload_checksums: true # Standaard; vereist door sommige S3-compatibele providers
```

### GCS

```yaml
warehouse:
  driver: files
  files:
    storage: gcs
    gcs:
      bucket: my-gcs-bucket
      creds_json: |
        { "type": "service_account", ... }
```

Laat `creds_json` leeg om Application Default Credentials (ADC) te gebruiken.

## Segmenten afstemmen

Segmenten worden verzegeld zodra de eerste van beide drempels wordt overschreden.

| Optie | Standaard | Beschrijving |
|---|---|---|
| `warehouse.files.max_segment_size` | `1073741824` (1 GiB) | Verzegel wanneer het actieve bestand deze grootte bereikt |
| `warehouse.files.max_segment_age` | `1h` | Verzegel wanneer het actieve bestand zo oud is |
| `warehouse.files.seal_check_interval` | `15s` | Hoe vaak de verzegelingstriggers worden geëvalueerd |

## Padsjabloon

Het files-warehouse schrijft data naar paden die worden gegenereerd uit een configureerbaar sjabloon. Zie [`--warehouse-files-path-template`](/nl/articles/config/#--warehouse-files-path-template) voor de standaardwaarde en aanpassingsopties.

**Beschikbare variabelen:**

| Variabele | Type | Beschrijving |
|---|---|---|
| `Table` | string | Geëscapete tabelnaam |
| `Schema` | string | 16-karakter schema-vingerafdruk |
| `SegmentID` | string | Segment-identifier (unixSeconds_uuid) |
| `Extension` | string | Bestandsextensie (csv of csv.gz) |
| `Year` | int | Jaar (bijv. 2026) |
| `Month` | int | Maandnummer (1-12) |
| `MonthPadded` | string | Maand met voorloopnul (01-12) |
| `Day` | int | Dag van de maand (1-31) |
| `DayPadded` | string | Dag met voorloopnul (01-31) |

**Voorbeeld:** `table={{.Table}}/year={{.Year}}/month={{.MonthPadded}}/day={{.DayPadded}}/{{.SegmentID}}.{{.Extension}}`

## Belangrijke opmerkingen

- **Spool vereist**: `storage.spool_enabled` moet `true` zijn. Het files-warehouse gebruikt de spool-directory om segmenten te stagen vóór de upload.
- **Schemamigraties**: `CreateTable` en `AddColumn` zijn no-ops. Het files-warehouse maakt of wijzigt geen tabellen. Schema-evolutie wordt afgehandeld door de consument van de bestanden.
- **Crashherstel**: Bij het opstarten scant d8a de spool-directory, verplaatst onderbroken uploads terug naar sealed en probeert ze opnieuw.
- **Upload-herhalingen**: Een segment dat niet kan worden geüpload, wordt tot 3 keer opnieuw geprobeerd. Na 3 mislukkingen wordt het naar een quarantine-directory verplaatst (`streams/<table>/<fingerprint>/failed/`) en niet opnieuw geprobeerd totdat het handmatig wordt afgehandeld.

## Je setup verifiëren

Start d8a na het configureren van het files-warehouse en controleer de logs. Je zou berichten moeten zien die aangeven dat segmenten worden verzegeld en geüpload naar je geconfigureerde bestemming.

## Je bestanden bevragen

De door deze driver geschreven bestanden kunnen rechtstreeks door de meeste analytics-warehouses worden geconsumeerd, zonder externe pipeline.

- **Snowflake** — Automatiseer ingestie met Snowpipe om nieuwe CSV/CSV.GZ-bestanden vanuit een external stage continu in een doeltabel te laden zodra ze binnenkomen. Dit elimineert volledig de noodzaak van een externe pipeline, hoewel het bevragen van een geconfigureerde External Table met auto-refresh een alternatief is als je geen dataverplaatsing wilt. Zie de [Snowflake Snowpipe-documentatie](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro.html) om dit op te zetten.

- **Amazon Redshift** — Gebruik Redshift Spectrum om een external table te maken en de bestanden rechtstreeks vanuit S3 te bevragen, met partition pruning als je je prefixes bijwerkt naar het standaardformaat `year=YYYY/month=MM/day=DD`. Spectrum vereist geen pipeline voor direct bevragen, maar Redshifts native auto-copy-feature kan nieuwe bestanden automatisch in managed storage ingesten zonder externe orchestrators. Bekijk de [Redshift Spectrum-documentatie](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html) voor de implementatiestappen.

- **Databricks SQL** — Gebruik Auto Loader om nieuwe CSV-bestanden incrementeel en automatisch te verwerken zodra ze in je cloudopslag landen. Dit dient als een native, pipeline-vrije ingestiemethode naar Delta-tabellen, maar het hernoemen van je prefixes naar standaard Hive-partitionering zal de initiële directory-discovery aanzienlijk optimaliseren. Lees de [Databricks Auto Loader-documentatie](https://docs.databricks.com/ingestion/auto-loader/index.html) voor de configuratie.

- **Azure Synapse Analytics** — Bevraag object storage rechtstreeks met Serverless SQL pools door een external table te maken of de `OPENROWSET`-functie op je bucket-pad te gebruiken. Dit vereist geen pipeline voor basisbevragingen, hoewel je voor optimale prestaties op grote datasets Azure Data Factory/Synapse Pipelines nodig hebt om de data naar Dedicated SQL pools te kopiëren. Verken de [Azure Synapse Serverless SQL-documentatie](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/query-data-storage) om te beginnen.

- **Starburst / Trino** — Verbind een Hive- of Iceberg-catalog met je bucket en definieer een external table rechtstreeks over je CSV-directory. Trino is een federated query engine en vereist daarom inherent geen ingestie-pipeline om de bestanden in-place te bevragen, maar het wijzigen van je directorystructuur naar `y=YYYY/m=MM/d=DD` wordt sterk aanbevolen voor efficiënte partition pruning. Bezoek de [Trino Hive Connector-documentatie](https://trino.io/docs/current/connector/hive.html) voor de setupdetails.

- **Apache Druid** — Ingest de CSV's continu met Druids native batch- of streaming-ingestie door een supervisor-spec naar het object storage-pad te wijzen. Er is geen externe pipeline nodig omdat Druid de ingestietaken intern beheert, maar je moet ervoor zorgen dat je CSV een timestamp-kolom bevat voor Druids verplichte primaire tijdspartitionering. Bekijk de [Apache Druid Ingestion-documentatie](https://druid.apache.org/docs/latest/ingestion/index.html) om je spec te configureren.

- **Firebolt** — Maak een external table die naar je bucket wijst en gebruik vervolgens standaard `INSERT INTO ... SELECT`-statements om data in een fact table te laden. Hoewel external tables direct bevragen zonder pipeline mogelijk maken, vereist het verplaatsen van data naar native Firebolt-tabellen enige externe orchestratie (zoals Airflow) om de ingestie van nieuwe bestanden regelmatig te triggeren. Meer informatie is beschikbaar in de [Firebolt External Tables-documentatie](https://docs.firebolt.io/sql-reference/commands/data-management/external-table.html).

- **Teradata Vantage** — Gebruik de Native Object Store (NOS)-feature om een foreign table rechtstreeks over je CSV/CSV.GZ-bestanden in cloudopslag te maken. Dit elimineert de noodzaak van een externe pipeline voor direct bevragen, hoewel het gebruik van standaard Hive-naamgevingsconventies (`$path/$var=...`) NOS in staat stelt partities efficiënt te filteren zonder elk bestand te scannen. De [Teradata Native Object Store-documentatie](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Foreign-Tables) beschrijft de vereiste syntaxis.

- **Google BigQuery** — Gebruik voor de beste ervaring in plaats daarvan de speciale [BigQuery-warehousedriver](/nl/articles/warehouses/bigquery). Als je de bestanden liever rechtstreeks consumeert, kun je ze bevragen door een external table over de bucket te maken, idealiter met je prefix bijgewerkt naar Hive-partitionering (`y=YYYY/m=MM/d=DD`) voor betere query-prestaties. Dit werkt volledig zonder pipeline, maar voor sneller bevragen bij frequente appends kun je beter native scheduled queries gebruiken om data naar gepartitioneerde native tabellen te verplaatsen. Raadpleeg de [BigQuery External Tables-documentatie](https://cloud.google.com/bigquery/docs/external-tables) voor meer details.

- **ClickHouse** — Gebruik voor de beste ervaring in plaats daarvan de speciale [ClickHouse-warehousedriver](/nl/articles/warehouses/clickhouse). Als je de bestanden liever rechtstreeks consumeert, kun je de `S3`-table engine of -functie gebruiken om ze te bevragen met glob-patronen zoals `y/m/d/*.csv.gz`. Een pipeline is niet strikt noodzakelijk, aangezien je een Materialized View over de S3-engine kunt opzetten om nieuwe data automatisch in een native MergeTree-tabel te ingesten, maar het toevoegen van het `y=YYYY/`-Hive-formaat verbetert het partitiefilteren. Lees meer in de [ClickHouse S3 Engine-documentatie](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3).

- **DuckDB** — Bevraag de bestanden rechtstreeks met de functie `read_csv` of `read_csv_auto` en glob-patronen (bijv. `SELECT * FROM read_csv_auto('/data/warehouse/events/**/*.csv')`). DuckDB vereist geen pipeline en geen server — het draait in-process, wat het ideaal maakt voor ad-hocanalyse of lichtgewicht lokale opstellingen. Installeer voor op S3 gehoste bestanden de `httpfs`-extensie en configureer je credentials met `SET s3_region`, `SET s3_access_key_id` en `SET s3_secret_access_key` vóór het bevragen. Zie de [DuckDB CSV-importdocumentatie](https://duckdb.org/docs/data/csv/overview.html) voor details.
