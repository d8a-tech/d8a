# Currency rate sources

d8a converts revenue columns using daily exchange rates from the open [currency-api](https://github.com/fawazahmed0/exchange-api). Rates are fetched in the background (every 6 hours by default) and stored as snapshots in `currency.destination_directory`, so a restart reuses the last good snapshot.

## Default sources

For each base currency, d8a tries these mirrors in order:

1. jsDelivr (`cdn.jsdelivr.net`)
2. Cloudflare Pages (`latest.currency-api.pages.dev`)

If every source fails, d8a keeps the previous snapshot. On a fresh install with no snapshot, converted currency columns stay null until a refresh succeeds.

## Add a fallback source

Use `currency.extra_source_urls` to add sources that are tried after the default mirrors. Each value is a base URL. d8a appends `/{currency}.json`, for example `/usd.json`, and expects the currency-api response shape:

```json
{ "date": "2026-11-03", "usd": { "eur": 0.89, "gbp": 0.77 } }
```

YAML:

```yaml
currency:
  extra_source_urls:
    - https://allratestoday.com/api/compat/currency-api/latest/v1/currencies
```

Environment variable or CLI flag:

```bash
CURRENCY_EXTRA_SOURCE_URLS="https://allratestoday.com/api/compat/currency-api/latest/v1/currencies"
d8a run --currency-extra-source-urls https://allratestoday.com/api/compat/currency-api/latest/v1/currencies
```

### Example: AllRatesToday

[AllRatesToday](https://allratestoday.com) serves a currency-api compatible endpoint (shown above) with ECB reference currencies. It covers the default base currencies. Other codes return 404 and fall through like any failed source. This example was contributed by the AllRatesToday team.

## Notes

- Every source is an outbound HTTPS request from your deployment. Add any extra hosts to your egress allowlist.
- In air-gapped mode, currency refreshes are disabled, so no source is contacted.
- To turn off background refreshes outside air-gapped mode, set `currency.refresh_interval` to `0`.
