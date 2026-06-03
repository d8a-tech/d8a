# Datalimieten

Dit artikel beschrijft de datalimieten die op velden in d8a worden toegepast.

## Limieten voor stringvelden

Alle stringvelden in d8a zijn onderworpen aan een hardcoded maximale lengte van **8.192 tekens**. Deze limiet wordt consistent afgedwongen over alle warehouse-backends (ClickHouse en BigQuery) om dataportabiliteit en voorspelbaar gedrag te garanderen.

Wanneer een stringwaarde deze limiet overschrijdt:
- De waarde wordt afgekapt tot 8.192 tekens
- Er wordt geen fout gegenereerd
- Het afkappen gebeurt voordat de data het warehouse bereikt


## Limieten voor hitgrootte

Individuele hits (trackingverzoeken) zijn onderworpen aan een configureerbare maximale groottelimiet. Deze limiet bepaalt de totale grootte van een enkel tracking-event, inclusief:
- URL- en queryparameters
- Request headers
- Request body
- Metadata
 

## Gerelateerde configuratie

Zie het artikel [Configuratie](/nl/articles/config) voor details over de beschikbare configuratieopties.
