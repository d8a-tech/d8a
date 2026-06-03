---
title: Technische verdieping
sidebar_position: 6
draft: true
---

Dit document legt uit hoe de d8a web tracker intern werkt. Het is bedoeld voor ontwikkelaars die de tracker-implementatie willen debuggen, uitbreiden of eraan willen bijdragen.

## Overzicht

Op hoofdlijnen doet de web tracker het volgende:

- Stelt een `gtag`-achtige API beschikbaar (`d8a('config' | 'event' | 'set' | 'consent', ...)`) via een globale functie en een queue.
- Consumeert in de wachtrij geplaatste commando's uit een data layer-array (standaard: `window.d8aLayer`).
- Zet commando's om in GA4 gtag-compatibele `/g/collect`-verzoeken.
- Beheert een kleine set first-party cookies (wanneer consent dit toestaat).
- Verrijkt verzoeken optioneel met User-Agent client hints (UA-CH) wanneer beschikbaar.
- Implementeert optionele "enhanced measurement" (automatisch gegenereerde events).

De runtime is bewust dependency-arm en wordt geleverd als bundles die met esbuild zijn gebouwd.

## Runtime-architectuur

De runtime bestaat uit een paar samenwerkende onderdelen:

- **Globale API (`d8a`)**: een kleine functie die aanroepen in de data layer-queue pusht.
- **Queue consumer**: leegt bestaande, in de wachtrij geplaatste aanroepen, patcht `push()` en routeert commando's naar handlers (`config`, `event`, enz.).
- **Dispatcher**: batcht events, bepaalt de effectieve configuratie (inclusief voorrang) en verzendt verzoeken.
- **Protocol mapper**: zet event-/contextdata om in GA4-achtige queryparameters.
- **Optionele helpers**: consent bridge, enhanced measurement, cookie-helpers.

## Control flow (van `d8a('event', ...)` naar een netwerkverzoek)

1. De gebruiker roept `d8a('event', ...)` aan (of er staat al een aanroep in de queue).
2. De queue consumer observeert het commando en werkt de runtime-status bij.
3. De dispatcher plaatst het event in de wachtrij om te verzenden.
4. Bij een flush doet de dispatcher het volgende:
   - selecteert de bestemmings-property-ID's (fan-out of `send_to`-routing),
   - bepaalt het cookie- + consent-gedrag,
   - bouwt een `/g/collect`-URL voor elke bestemming,
   - verzendt verzoeken met `fetch(..., { keepalive: true, mode: 'no-cors' })` (met beperkte retries/backoff).

## Pakketstructuur

De broncode van de web tracker staat in `js/web-tracker/src/`. Hij is georganiseerd per verantwoordelijkheid (zie elke directory voor de actuele, volledige lijst met modules):

- **Entrypoints**: `src/index.js` (ESM-exportoppervlak) en `src/browser_entry.js` (entry van de script-tag-bundle).
- **Installer**: `src/install.js` bedraadt de runtime en stelt `installD8a` beschikbaar.
- **Runtime**: `src/runtime/` bevat de in-browser runtime. De belangrijkste "ruggengraat" is:
  - `src/runtime/queue_consumer.js` (leest commando's en muteert de runtime-status)
  - `src/runtime/dispatcher.js` (batcht en verzendt verzoeken)
  - `src/runtime/state.js` (vorm van de status + standaardwaarden)
- **GA4-mapping**: `src/ga4/` bouwt request-payloads (bijvoorbeeld `src/ga4/gtag_mapper.js`).
- **Cookies**: `src/cookies/` definieert cookieformaten en schrijfgedrag (bijvoorbeeld `src/cookies/d8a_cookies.js`, `src/cookies/identity.js`).
- **Transport en utilities**: `src/transport/` en `src/utils/` bevatten send- + URL-helpers (bijvoorbeeld `src/transport/send.js`, `src/utils/endpoint.js`).

## Developer experience

De tracker-runtime is bewust dependency-arm. De meeste modules zijn geschreven als kleine factories die geïnjecteerde dependencies accepteren (bijvoorbeeld `windowRef`) om de code testbaar te houden onder Node en eenvoudig te debuggen.

### Browsercompatibiliteit

De gepubliceerde bundles richten zich op **ES2018** en vertrouwen op moderne browser-API's zoals `fetch` en `URL`. Dit houdt de runtime klein en vermijdt zware transpilatie.

### User-Agent client hints (UA-CH)
Wanneer beschikbaar gebruikt de web tracker de UA-CH-API (`navigator.userAgentData.getHighEntropyValues`) om verzoeken te verrijken met device- en platformmetadata van hogere kwaliteit. De implementatie is feature-detected en wordt per page load gecached; de tracker valt terug op het reguliere, door de browser afgeleide gedrag wanneer UA-CH niet beschikbaar is (bijvoorbeeld in browsers die de API niet ondersteunen).

### TypeScript-ondersteuning

De broncode van de runtime is **ESM JavaScript**, en het pakket levert **TypeScript type-declaraties** via `js/web-tracker/index.d.ts`.

### Waarom deze structuur

- **Bijdragen met weinig wrijving**: gewone ESM JavaScript + esbuild houdt de toolchain klein.
- **Testbaarheid**: dependency injection (`windowRef`, `document`, enz.) maakt deterministische tests mogelijk zonder echte browser.
- **Debugbaarheid**: er worden sourcemaps gegenereerd voor de bundles.

## Build- en testworkflow

- **Build**: het pakket gebruikt esbuild om geminificeerde bundles in `dist/` te produceren (script-tag-bundle + ESM-bundle).
- **Tests**: tests gebruiken Node's ingebouwde runner onder `js/web-tracker/test/`.
