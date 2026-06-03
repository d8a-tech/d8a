# ClickHouse

ClickHouse is een snel, open-source kolomgeoriënteerd databasebeheersysteem. d8a configureren om ClickHouse te gebruiken is eenvoudig en vereist geen externe setup.

## Configuratie

:::info Tip
   De volledige configuratiereferentie is [hier](/nl/articles/config#--warehouse-clickhouse-database) beschikbaar.
:::

Voeg het volgende toe aan je `config.yaml`-bestand:

```yaml
warehouse:
  driver: clickhouse
  clickhouse:
    host: localhost
    port: "9000"
    database: d8a
    username: default
    password: "your-password"
```

## Belangrijke opmerkingen

- **Engine-ondersteuning**: d8a ondersteunt momenteel alleen de `MergeTree`-engine. Tabellen worden aangemaakt met `ENGINE = MergeTree()`.
- **Distributed/Replicated-opstellingen**: Distributed- en Replicated-tabelopstellingen worden op dit moment niet ondersteund. Gebruik één enkele ClickHouse-instantie of vraag deze feature aan via GitHub issues.
- **Nullability**: Nullable kolommen uit het schema worden in ClickHouse opgeslagen als `NOT NULL` met `DEFAULT`. Dit voorkomt de [`Nullable(T)`-opslagoverhead](https://clickhouse.com/docs/optimize/avoid-nullable-columns) terwijl de semantische nullability behouden blijft. Ontbrekende of `nil`-waarden worden automatisch omgezet naar type-specifieke standaardwaarden (bijv. `''` voor strings, `0` voor getallen, `'1970-01-01'` voor datums).

## Metadata

ClickHouse-specifieke optimalisaties kunnen worden toegepast via Arrow-veldmetadata (deze informatie is bruikbaar voor ontwikkelaars die nieuwe kolommen implementeren; dit kan momenteel niet vanuit de configuratie worden bestuurd):

| Metadata-sleutel | Waarde | Beschrijving |
|--------------|-------|-------------|
| `clickhouse.low_cardinality` | `"true"` | Wikkelt het kolomtype als `LowCardinality(T)` voor betere opslagefficiëntie en query-prestaties op kolommen met lage cardinaliteit. Van toepassing op elk door ClickHouse ondersteund type; |
| `clickhouse.codec` | `"CODEC(Delta, ZSTD)"` | Voegt een CODEC-clausule toe aan de kolomdefinitie in de ClickHouse-DDL. De waarde moet een volledige CODEC-clausule zijn (bijv. `CODEC(Delta, ZSTD)` of `CODEC(Delta)`). Gebruik de helperfunctie `meta.Codec(codec, compressionAlg)` om de waarde te genereren. Codec-metadata heeft geen invloed op de semantische schema-compatibiliteitscontroles. |

## Je setup verifiëren

Start d8a na het configureren van ClickHouse en controleer de logs. Je zou berichten moeten zien die een succesvolle verbinding met ClickHouse aangeven.
