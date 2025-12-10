# Data limits

This article describes data limits applied to fields in d8a.

## String field limits

All string fields in d8a are subject to a hardcoded maximum length of **8,192 characters**. This limit is enforced consistently across all warehouse backends (ClickHouse and BigQuery) to ensure data portability and predictable behavior.

When a string value exceeds this limit:
- The value is truncated to 8,192 characters
- No error is raised
- The truncation happens before data reaches the warehouse


## Hit size limits

Individual hits (tracking requests) are subject to a configurable maximum size limit. This limit controls the total size of a single tracking event, including:
- URL and query parameters
- Request headers
- Request body
- Metadata
 

## Related configuration

See the [Configuration](./config.md) article for details on available configuration options.

