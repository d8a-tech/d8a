# Geneste parameters afvlakken

Met flattening nested parameters kun je waarden uit protocolspecifieke, herhaalde structuren halen en ze naar custom kolommen op het hoogste niveau schrijven.

## Minimale voorbeeldconfiguratie

### YAML-configuratiebestand

```yaml
ga4:
  params:
    - name: campaign_id
matomo:
  custom_dimensions:
    - slot: 3
      name: plan_tier
  custom_variables:
    - name: ab_test_group
```

## GA4: Event params

Gebruik `ga4.params` om event-scoped kolommen te bouwen uit GA4-`params`-entries (`ep.*` en `epn.*`).

Elke entry ondersteunt:
- **name**: De te matchen parametersleutel (verplicht)
- **column_name**: Override voor de naam van de outputkolom (optioneel; standaard `params_<name>`). Indien ingesteld wordt deze waarde als naam van de outputkolom gebruikt.
- **type**: `string`, `int64` of `float64` (optioneel; standaard `string`). Dit bepaalt de waardecasting en de selectie van het bronwaardeveld.

Voorbeeld:

```yaml
ga4:
  params:
    - name: campaign_id
      type: string
```

## Matomo: Custom dimensions

Gebruik `matomo.custom_dimensions` om kolommen te bouwen uit Matomo-`dimensionN`-waarden.

Elke entry ondersteunt:
- **slot**: Slotnummer van de dimension (verplicht)
- **name**: Naamlabel van de dimension (verplicht)
- **column_name**: Override voor de naam van de outputkolom (optioneel; standaard `custom_dimension_<name>`). Indien ingesteld wordt deze waarde als naam van de outputkolom gebruikt.
- **scope**: `event` of `session` (optioneel; standaard `event`). Dit bepaalt of de waarde per event wordt gelezen of op session-scope wordt bepaald.

Voorbeeld:

```yaml
matomo:
  custom_dimensions:
    - slot: 3
      name: plan_tier
      scope: session
```

## Matomo: Custom variables

Gebruik `matomo.custom_variables` om kolommen te bouwen uit Matomo custom variables (`cvar` en `_cvar`).

Elke entry ondersteunt:
- **name**: De te matchen sleutel van de custom variable (verplicht)
- **column_name**: Override voor de naam van de outputkolom (optioneel; standaard `custom_variable_<name>`). Indien ingesteld wordt deze waarde als naam van de outputkolom gebruikt.
- **scope**: `event` of `session` (optioneel; standaard `event`). Dit bepaalt of de waarde per event wordt gelezen of op session-scope wordt bepaald.

Voorbeeld:

```yaml
matomo:
  custom_variables:
    - name: ab_test_group
      scope: event
```

## Overige configuratieopties

Je kunt dezelfde instellingen ook configureren met cli-flags of omgevingsvariabelen.

### Inline flags (CLI/omgevingsvariabelen)

```bash
export GA4_PARAMS='[{"name":"campaign_id","type":"string"}]'
export MATOMO_CUSTOM_DIMENSIONS='[{"slot":3,"name":"plan_tier","column_name":"plan_tier_custom","scope":"event"}]'
export MATOMO_CUSTOM_VARIABLES='[{"name":"ab_test_group","column_name":"ab_group_custom","scope":"session"}]'
```

Of via de CLI:

```bash
./d8a run \
  --ga4-params '[{"name":"campaign_id","type":"string"}]' \
  --matomo-custom-dimensions '[{"slot":3,"name":"plan_tier","scope":"event"}]'
```

### Voorrangsregels

Wanneer zowel YAML als inline flags worden opgegeven:
- `ga4.params`: Flag- en env-waarden worden toegevoegd aan de YAML-entries
- `matomo.custom_dimensions`: Flag- en env-waarden worden toegevoegd aan de YAML-entries
- `matomo.custom_variables`: Flag- en env-waarden worden toegevoegd aan de YAML-entries

## Gerelateerde configuratie

Zie de [Configuratie](/nl/articles/config)-referentie voor alle beschikbare configuratieopties.
