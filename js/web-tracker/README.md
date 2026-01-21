# Web tracker

The d8a web tracker provides a GA4 gtag-style API that sends GA4 gtag-compatible requests directly to a d8a collector.

## Installation (script tag)

The script-tag bundle auto-installs on load and supports optional `src` query parameters:

- `?l=<name>`: d8a queue name (data layer)
- `?g=<name>`: d8a global function name
- `?gl=<name>`: gtag/GTM consent queue name (defaults to `dataLayer`)

```html
<script async src="https://cdn.jsdelivr.net/npm/@d8a-tech/wt/dist/wt.min.js"></script>
<script>
  window.d8aLayer = window.d8aLayer || [];
  window.d8a = window.d8a || function(){d8aLayer.push(arguments);};

  d8a("js", new Date());
  d8a("config", "<property_id>", {
    server_container_url: "https://example.org/d/c",
  });
</script>
```

## Installation (npm module)

```bash
npm install @d8a-tech/wt
```

```javascript
import { installD8a } from "@d8a-tech/wt";

// Optional overrides:
// - dataLayerName: d8a queue name (defaults to "d8aLayer")
// - globalName: d8a global function name (defaults to "d8a")
// - gtagDataLayerName: gtag/GTM consent queue name (defaults to "dataLayer")
installD8a();

const d8a = window.d8a;
if (!d8a) throw new Error("d8a is not installed");

d8a("js", new Date());
d8a("config", "<property_id>", {
  server_container_url: "https://global.t.d8a.tech/<property_id>/d/c",
});
```

## Configuration precedence

When the same key is provided in multiple places, we resolve values as:

- Event params > config params > set params > browser defaults

## Development

- Runtime source is ESM JavaScript in `src/` and types are shipped in `index.d.ts`.
- Tests use `tsx` (Node test runner compatibility):

```bash
cd js/web-tracker
npm test
```

## Production build

This package can produce a single browser bundle:

- `dist/wt.min.js`: Script-tag bundle (auto-installs on load)
- `dist/index.min.mjs`: ESM bundle

Build:

```bash
cd js/web-tracker
npm install
npm run build
```

## Examples (dev vs prod)

The `example/` directory contains two pages to quickly validate behavior in a browser:

- `example/index.html`: **dev/local** setup (served by Vite; imports from `../src/` and calls `installD8a()`).
- `example/prod.html`: **production-like** setup (loads `../dist/wt.min.js`, which auto-installs on load).

### Dev example (no build step)

Run:

```bash
cd js/web-tracker
npm install
npm run dev
```

Then open the printed URL (Vite will serve `example/index.html` as `/`).

### Prod-like example (requires build)

Run:

```bash
cd js/web-tracker
npm install
npm run build
python3 -m http.server 8080
```

Then open:

- `http://127.0.0.1:8080/example/prod.html`
