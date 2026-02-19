# GA4 Duplicator

Intercepts Google Analytics 4 (GA4) collect requests and duplicates them to a D8A server endpoint.

## What it does

This library provides browser-based network interception to duplicate GA4 analytics calls. It intercepts requests made to Google Analytics and forwards them to a configured D8A server endpoint, enabling parallel data collection.

Supported interception methods:
- Fetch API requests
- XMLHttpRequest calls
- navigator.sendBeacon calls
- Script tag loads

## Credentialed duplicate requests

Duplicate requests sent via `fetch` include credentials (`credentials: "include"`). This allows
cookie-based features such as server-side Google Tag Manager debug mode to work when the duplicate
endpoint relies on cookies.

Notes:
- When the original request uses `navigator.sendBeacon` and `convert_to_get` is `false`, duplicate 
  requests will also use `sendBeacon` (which cannot send credentials). Use `convert_to_get: true` 
  to send beacon duplicates via `fetch` with credentials included.
- For cross-origin duplicate endpoints, your server must support credentialed CORS:
  - `Access-Control-Allow-Origin` must be an explicit origin (not `*`).
  - `Access-Control-Allow-Credentials` must be `true`.
- If credentialed CORS is not allowed by the endpoint, browsers can fail duplicate requests due to
  CORS policy.
- Only configure trusted destination endpoints you control, because credentialed requests may send
  cookies scoped to that destination origin.

## Usage

Include the built script in your HTML and initialize the duplicator:

```html
<script src="dist/gd.min.js"></script>
<script>
  window.createGA4Duplicator({
    server_container_url: "https://your-d8a-endpoint.com",
    debug: false
  });
</script>
```

### Configuration Options

- `server_container_url`: Default D8A server endpoint URL. Can be overridden for each destination.
- `destinations`: Array of destination objects with `measurement_id`, `server_container_url`, and optional `convert_to_get` (default: []).
- `debug`: Enable debug logging (default: false).
- `convert_to_get`: Convert POST requests into multiple GET requests (default: false). This is useful for environments that don't support POST or when you want to split batched requests into individual hits.

### Multiple Destinations

```javascript
window.createGA4Duplicator({
  server_container_url: "https://default-endpoint.com",
  destinations: [
    {
      measurement_id: "G-ABC123",
      server_container_url: "https://endpoint1.com",
    },
    {
      measurement_id: "G-XYZ789",
      server_container_url: "https://endpoint2.com",
      convert_to_get: true
    }
  ]
});
```

## Build

### Production build (minified):
```bash
npm run build:prod
```
Output: `dist/gd.min.js`

### Development build:
```bash
npm run build:dev
```
Output: `dist/gd.js`

## Source Code Hashing

The build process generates a SHA256 hash of all TypeScript source files in the `src/` directory and saves it to `src.hash`. This ensures build integrity and enables CI verification.

### Verify source code integrity:
```bash
npm run hash
```

This command compares the stored hash with the current source code hash:
- **Exit code 0**: Source code matches the hash (build is up-to-date)
- **Exit code 1**: Source code has changed since last build

Use this in CI pipelines to ensure `dist/` files are regenerated when source code changes.

## Test

### Unit tests:
```bash
npm test
```

### End-to-end tests:

1. Install Playwright browsers (first time only):
```bash
npx playwright install chromium
```

2. Run e2e tests:
```bash
npm run test:e2e
```

The e2e tests start a local HTTP server and use Playwright to verify that GA4 requests are properly duplicated to the D8A endpoint.

### Manual testing:

Open `test.html` in a browser to manually test different network interception methods. The page includes buttons to trigger various types of GA4 requests and displays logs of duplication activity.

## Notes

### sendBeacon Credential Limitation

When `navigator.sendBeacon` is used for the original GA4 request and `convert_to_get` is set to `false`, duplicate requests will be sent using the original `sendBeacon` method, which does not support sending cookies or credentials. This means cookie-based features (such as server-side Google Tag Manager debug mode) will not work with beacon duplicates in this configuration.

**Workaround:** Set `convert_to_get: true` for destinations that require cookies with beacon requests. This will convert beacon requests to fetch requests with credentials included, ensuring cookies are sent with the duplicate request.

