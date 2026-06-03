# Verkeersfiltering

Met traffic filtering kun je tracking-events uitsluiten of toestaan op basis van voorwaarden die tijdens de hitverwerking worden geëvalueerd, vóórdat er naar het warehouse wordt geschreven.

## Structuur van een filtervoorwaarde

Elke filtervoorwaarde bestaat uit:
- **name**: String-identifier
- **type**: `exclude` (blokkeer overeenkomend verkeer) of `allow` (sta alleen overeenkomend verkeer toe)
- **test_mode**: Wanneer `true`, stelt dit de kolomwaarde `traffic_filter_name` in zonder events op te nemen of uit te sluiten
- **expression**: Expressie die wordt geëvalueerd tegen de beschikbare velden

Wanneer er een `allow`-filter bestaat, wordt alleen verkeer verwerkt dat aan ten minste één allow-voorwaarde voldoet. Exclude-filters blokkeren overeenkomend verkeer.

## Configuratie

### YAML-configuratiebestand

```yaml
filters:
  fields:
    - ip_address

  conditions:
    - name: "internal_traffic"
      type: exclude
      test_mode: false
      expression: 'ip_address startsWith "192.168"'
```

### Inline flags (CLI/omgevingsvariabelen)

```bash
export FILTERS_FIELDS="ip_address"
export FILTERS_CONDITIONS='{"name":"internal_traffic","type":"exclude","test_mode":false,"expression":"ip_address startsWith \"192.168\""}'
```

Of via de CLI:
```bash
./tracker-api run --filters-conditions '{"name":"internal_traffic","type":"exclude","test_mode":false,"expression":"ip_address == \"10.0.0.1\""}'
```

### Voorrangsregels

Wanneer zowel YAML als inline flags worden opgegeven:
- `filters.fields`: De flag-waarde vervangt de YAML-waarde
- `filters.conditions`: De flag-waarden worden toegevoegd aan de YAML-voorwaarden

## Beschikbare velden

De configuratie `filters.fields` bepaalt welke event-kolommen in de expressie-omgeving worden geïnjecteerd.

## Expressie-interpreter

Filterexpressies worden geëvalueerd met [expr-lang](https://expr-lang.org/), een snelle expressietaal voor Go. Zie de [expr language definition](https://expr-lang.org/docs/language-definition) voor de volledige syntaxisreferentie.

## API-referentie

d8a biedt de volgende custom functie voor filterexpressies. Alle standaard expr-lang-operatoren en stringfuncties zijn ook beschikbaar — zie [expr-lang string functions](https://expr-lang.org/docs/language-definition#string-functions) voor de volledige referentie.

### Netwerkfuncties

#### `inCidr(ip string, cidr string) bool`
Geeft `true` terug als IP-adres `ip` binnen het CIDR-bereik `cidr` valt. Ondersteunt zowel IPv4 als IPv6.

```yaml
expression: 'inCidr(ip_address, "10.0.0.0/8")'
```

### Standaard expr-operatoren

Alle standaard expr-lang-operatoren zijn beschikbaar, waaronder:
- Vergelijking: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logisch: `&&` (and), `||` (or), `!` (not)
- Rekenkundig: `+`, `-`, `*`, `/`, `%`
- Ternair: `condition ? true_value : false_value`
- String: `contains`, `startsWith`, `endsWith`, `matches` (regex)

Voorbeelden:

```yaml
expression: 'ip_address startsWith "192.168"'
expression: 'hostname endsWith ".internal"'
expression: 'ip_address matches "^10\\.0\\.0\\.[0-9]{1,3}$"'
expression: 'user_agent contains "bot"'
```

Zie de [expr-lang language definition](https://expr-lang.org/docs/language-definition) voor de volledige syntaxis- en functiereferentie.

## Voorbeelden

### Intern verkeer blokkeren
```yaml
- name: "internal_traffic"
  type: exclude
  test_mode: false
  expression: 'inCidr(ip_address, "192.168.0.0/16") || inCidr(ip_address, "10.0.0.0/8")'
```

### Test mode voor kantoorverkeer
```yaml
- name: "office_traffic"
  type: exclude
  test_mode: true  # Stelt traffic_filter_name in zonder uit te sluiten
  expression: 'ip_address == "203.0.113.50"'
```

### Alleen specifieke events toestaan
```yaml
- name: "page_views_only"
  type: allow
  test_mode: false
  expression: 'event_name == "page_view" || event_name == "screen_view"' # vereist event_name in filters.fields
```

### Complexe voorwaarde met meerdere velden
```yaml
filters:
  fields:
    - ip_address
    - user_id

  conditions:
    - name: "internal_test_users"
      type: exclude
      test_mode: false
      expression: 'user_id startsWith "test_" && inCidr(ip_address, "10.0.0.0/8")'
```

## Gerelateerde configuratie

Zie de [Configuratie](/nl/articles/config)-referentie voor alle beschikbare configuratieopties.
