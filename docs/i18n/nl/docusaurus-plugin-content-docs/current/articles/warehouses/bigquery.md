# BigQuery

Deze gids legt uit hoe je d8a configureert om Google BigQuery als data warehouse te gebruiken. BigQuery is Google's cloud data warehouse dat grootschalige analytics-workloads aankan.

:::caution Gratis tier beschikbaar
BigQuery biedt een gratis tier die het volgende omvat:

- **Opslag**: De eerste 10 GiB per maand is gratis
- **Queries (analyse)**: De eerste 1 TiB verwerkte querydata per maand is gratis

Zie voor meer details de [documentatie over de gratis BigQuery-tier](https://cloud.google.com/bigquery/pricing?hl=en#free-usage-tier).
:::

## Wat je nodig hebt

Zorg vóór het configureren van BigQuery dat je het volgende hebt:

- Een Google Cloud Platform (GCP)-account
- Een GCP-project met de BigQuery API ingeschakeld
- Een GCP-project-ID (te vinden in de Google Cloud Console)
- Een BigQuery-dataset

D8a ondersteunt twee authenticatiemethoden voor BigQuery:

- **OAuth 2.0**: Verbind met je persoonlijke Google-account. Dit verleent schrijftoegang tot BigQuery in al je Google-projecten. Geschikt voor persoonlijk gebruik.
- **Service Account**: Upload een service account JSON-sleutelbestand. Maakt het instellen van fijnmazigere permissies mogelijk, geschikt voor organisaties.

We lopen hieronder beide opties door.

## Credentials verkrijgen

D8a ondersteunt twee authenticatiemethoden. Kies degene die het beste bij je behoeften past:

### Optie 1: OAuth 2.0 (alleen Cloud)

Met OAuth 2.0 kun je verbinden met je persoonlijke Google-account. Deze methode is eenvoudiger op te zetten en vereist geen service account.

**Hoe het werkt:**

1. Klik in de d8a-UI op **Authorize with Google (OAuth 2)**
2. Er opent een nieuw venster waarin je je bij je Google-account moet aanmelden
3. Bekijk en verleen de gevraagde permissies
4. Het venster sluit automatisch en je integratie wordt aangemaakt

:::warning
OAuth-integraties worden gedeeld op tenant-niveau. Zodra je een OAuth-integratie aanmaakt, wordt deze beschikbaar voor alle gebruikers in je tenant die toegang hebben tot integraties. Dit betekent dat je collega's dezelfde integratie kunnen selecteren en gebruiken om hun eigen BigQuery-warehouseverbindingen te configureren met andere project-, dataset- en tabelinstellingen. De OAuth-credentials verlenen schrijftoegang tot BigQuery in alle Google-projecten die aan het geauthenticeerde Google-account zijn gekoppeld. Daarom wordt aanbevolen OAuth 2.0 te gebruiken voor persoonlijke tenants.
:::

### Optie 2: Service Account (Cloud en On-premises)

Met service accounts kun je fijnmazigere permissies instellen; ze zijn beter geschikt voor organisatorisch gebruik. Zo maak je er een aan:

#### Stap 1: Maak het service account aan

1. Open de [Google Cloud Console](https://console.cloud.google.com/)
2. Selecteer je project in de dropdown bovenaan
3. Ga naar **IAM & Admin** → **Service Accounts** (of zoek op "Service Accounts" in de zoekbalk bovenaan)
4. Klik op de knop **+ CREATE SERVICE ACCOUNT**
5. Geef het een naam (bijv. `d8a-bigquery`) en voeg optioneel een beschrijving toe
6. Klik op **CREATE AND CONTINUE**

#### Stap 2: Verleen permissies

1. Zoek en selecteer in de sectie "Grant this service account access to project" de rol **BigQuery Admin**
   - Je kunt "BigQuery Admin" in het zoekvak voor rollen typen om hem snel te vinden
2. Klik op **CONTINUE** en vervolgens op **DONE**

:::note
Deze rol is vereist omdat hij d8a de nodige permissies verleent om tabellen aan te maken en te wijzigen, data te schrijven en BigQuery-resources te beheren.
:::

#### Stap 3: Download de sleutel

1. Zoek je zojuist aangemaakte service account in de lijst en klik erop
2. Ga naar het tabblad **KEYS**
3. Klik op **ADD KEY** → **Create new key**
4. Selecteer **JSON** als sleuteltype
5. Klik op **CREATE** — hiermee wordt een JSON-bestand naar je computer gedownload

:::caution Belangrijk
Houd credentials veilig. Dit bestand bevat credentials die toegang geven tot je BigQuery-data. Je moet dit bestand in de volgende stap in de d8a-UI uploaden.
:::

## Credentials configureren

### Stap 1: Verkrijg je project-ID en datasetnaam

- **Project-ID**: Te vinden in de Google Cloud Console bovenaan de pagina, of in het gedownloade JSON-bestand (zoek naar het veld `project_id`).
- **Datasetnaam**: De naam van je BigQuery-dataset. Heb je er nog geen, [maak hem dan aan in de BigQuery-console](https://docs.cloud.google.com/bigquery/docs/datasets#create-dataset).

### Stap 2: Voeg de configuratie toe

Kies de optie die past bij je installatietype (Cloud of On-premises):

#### Optie A: Cloud-installatie

Navigeer in de d8a-UI naar de BigQuery-configuratiesectie. Je ziet twee authenticatieopties:

**Met OAuth 2.0:**

1. Klik op **Authorize with Google (OAuth 2)**
2. Meld je aan bij je Google-account in het popupvenster
3. Bekijk en verleen de gevraagde permissies
4. Het venster sluit automatisch zodra de autorisatie is voltooid

**Met Service Account:**

1. Klik op **Choose JSON file** onder "Upload Service Account JSON key"
2. Selecteer het eerder gedownloade JSON-bestand

Klik na het kiezen van je authenticatiemethode op next en geef de volgende velden op:

- **Database name** \* (verplicht)

  - Jouw naam voor de BigQuery-verbinding die je in de d8a-UI ziet

- **BigQuery Project ID** \* (verplicht)

  - Je GCP-project-ID (genoemd in Stap 1 hierboven)

- **BigQuery Dataset ID** \* (verplicht)

  - Je BigQuery-dataset-ID (genoemd in Stap 1 hierboven)

- **BigQuery Table Name** (optioneel)

  - De tabel waarin je de data wilt opslaan (standaard `events`)

**Je setup verifiëren:**

Ga naar de [BigQuery-console](https://console.cloud.google.com/bigquery) en controleer of je tabel is aangemaakt met het juiste d8a-schema. De documentatie over het [databaseschema](/nl/articles/database-schema) beschrijft alle beschikbare kolommen.

#### Optie B: On-premises-installatie

:::info Tip
De volledige configuratiereferentie is [hier](/nl/articles/config#--warehouse-bigquery-creds-json) beschikbaar.
:::

Open je `config.yaml`-bestand en voeg de BigQuery-configuratie toe:

```yaml
warehouse:
  driver: bigquery
  bigquery:
    project_id: your-gcp-project-id
    dataset_name: your-dataset-name
    # Paste the contents of the downloaded JSON file here
    creds_json: |
      {
        "type": "service_account",
        ...
      }
```

:::info Tip
Zorg dat de inspringing correct is. De JSON moet met spaties zijn ingesprongen zodat hij onder `creds_json:` uitlijnt. De `|` na `creds_json:` maakt multiregel-strings in YAML mogelijk.
:::

**Je setup verifiëren:**

Start d8a na het configureren van BigQuery en controleer de logs. Je zou berichten moeten zien die een succesvolle verbinding met BigQuery aangeven.

Je kunt ook naar de [BigQuery-console](https://console.cloud.google.com/bigquery) gaan en controleren of je tabel is aangemaakt met het juiste d8a-schema. De documentatie over het [databaseschema](/nl/articles/database-schema) beschrijft alle beschikbare kolommen.
