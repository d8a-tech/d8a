# Verkeersattributie

Dit artikel legt uit hoe d8a bepaalt waar je verkeer vandaan komt door elke session te classificeren met **Source**, **Medium** en **Term**.

## Wat zijn Source, Medium en Term?

- **Source**: De oorsprong van het verkeer (bijv. `google`, `facebook`, `vimeo`, `direct`).
- **Medium**: Het marketingkanaal (bijv. `organic`, `cpc`, `social`, `email`, `referral`).
- **Term**: Het zoekwoord of de campagneterm (indien beschikbaar via zoekmachines of UTM-tags).

Deze attributen volgen de standaard analytics-conventies en helpen je de samenstelling van je verkeer te begrijpen.

## Hoe detectie werkt

Attribution wordt eenmaal per session berekend, waarbij het eerste event (meestal de landingspaginaweergave) wordt geanalyseerd. Het systeem onderzoekt:

- De landingspagina-URL (met alle queryparameters intact).
- De HTTP-referrer (de pagina waar de bezoeker vandaan kwam).
- UTM-trackingparameters (`utm_source`, `utm_medium`, `utm_term`).
- Click-identifiers van advertentienetwerken (`gclid`, `fbclid`, `msclkid`, enz.).

Detectie volgt een prioriteitsvolgorde. De eerste overeenkomende regel bepaalt de initiële source/medium/term, waarna UTM-parameters individuele velden overschrijven indien aanwezig.

## Detectieregels (in prioriteitsvolgorde)

### 1. Click-id's van betaalde advertenties

Sessions met landingspagina-URL's die specifieke queryparameters bevatten, worden direct geclassificeerd als betaald verkeer (medium=cpc). Voorbeelden:

| Parameter | Source | Medium |
|:----------|:-------|:-------|
| `gclid`, `gbraid`, `wbraid` | google | cpc |
| `msclkid` | bing | cpc |
| `fbclid` | facebook | cpc |

Een volledige lijst is te vinden in de broncode: [`pkg/columns/sessioncolumns/session_smt_source.go`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/session_smt_source.go).

### 2. Videoplatforms

Referrers van videohostingsites worden geclassificeerd als `medium=video`. Sources zijn onder andere YouTube, Vimeo, Dailymotion, Twitch en andere (volledige lijst in [`pkg/columns/sessioncolumns/smt/video.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/video.yaml)).

### 3. E-mailproviders

Referrers van webmail-interfaces worden geclassificeerd als `medium=email`. Dit omvat Gmail, Outlook, Yahoo Mail, ProtonMail en andere (volledige lijst in [`pkg/columns/sessioncolumns/smt/emails.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/emails.yaml)).

Daarnaast wordt elke referrer die `mail.` in de hostnaam bevat (bijv. `mail.company.com`) automatisch als e-mail behandeld, met het genormaliseerde domein als source.

### 4. Social media

Referrers van sociale netwerken worden geclassificeerd als `medium=social`. Dit omvat Facebook, Instagram, Twitter/X, LinkedIn, Reddit, TikTok, Pinterest en vele andere (volledige lijst in [`pkg/columns/sessioncolumns/smt/socials.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/socials.yaml)).

### 5. AI-assistenten en zoeken

Referrers van AI-chatinterfaces worden geclassificeerd als `medium=ai`. Dit omvat ChatGPT, Claude, Perplexity en andere (volledige lijst in [`pkg/columns/sessioncolumns/smt/ai.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/ai.yaml)).

### 6. Zoekmachines

Referrers van zoekmachines worden geclassificeerd als `medium=organic`. Dit omvat Google, Bing, DuckDuckGo, Yahoo, Baidu, Yandex en honderden regionale/gespecialiseerde zoekmachines (volledige lijst in [`pkg/columns/sessioncolumns/smt/searchengines.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/searchengines.yaml)).

Het systeem probeert de zoekterm uit de queryparameters van de referrer te halen (bijv. `q`, `query`, `p`). De meeste moderne browsers verwijderen echter queryparameters uit referrers omwille van privacy, dus termen zijn zelden beschikbaar tenzij ze expliciet via UTM-tags worden meegegeven.

### 7. Generieke referral

Elke externe referrer (van een ander domein dan je site) die niet aan bovenstaande categorieën voldoet, wordt geclassificeerd als `medium=referral`, met het genormaliseerde domein als source.

### 8. Direct verkeer

Als er geen referrer bestaat en er geen betaalde click-id's aanwezig zijn, wordt verkeer geclassificeerd als `source=direct`, `medium=none`. Dit omvat:

- De URL rechtstreeks in de browser typen
- Op bladwijzers klikken
- Links vanuit native mobiele apps
- Links vanuit documenten (PDF's, Office-bestanden)
- Overgangen van beveiligd (HTTPS) naar onbeveiligd (HTTP) die referrers verwijderen

## UTM-parameteroverrides

Je kunt attribution handmatig sturen door UTM-tags aan je URL's toe te voegen:

```
https://yoursite.com/?utm_source=newsletter&utm_medium=email&utm_term=spring_sale
```

UTM-parameters overschrijven altijd de automatisch gedetecteerde waarden:

- `utm_source` → overschrijft Source
- `utm_medium` → overschrijft Medium  
- `utm_term` → overschrijft Term

Elke parameter wordt afzonderlijk toegepast. Als je alleen `utm_source` opgeeft, blijven het gedetecteerde medium en de term ongewijzigd.

**Voorbeeld**: Een bezoeker klikt op een link in een YouTube-videobeschrijving naar `yoursite.com/?utm_source=youtube_channel&utm_medium=video_description`. De referrer zou normaal gesproken `source=youtube, medium=video` detecteren, maar de UTM-tags overschrijven dit naar `source=youtube_channel, medium=video_description`.

## Veelvoorkomende scenario's

### Organisch zoeken via Google
- Referrer: `https://www.google.com/`
- Resultaat: `source=google, medium=organic`
- Let op: Zoekterm meestal niet beschikbaar vanwege referrer-privacy.

### Google Ads-klik
- Landingspagina: `https://yoursite.com/?gclid=ABC123XYZ`
- Resultaat: `source=google, medium=cpc`
- De `gclid` activeert directe classificatie als betaald.

### Organische Facebook-post
- Referrer: `https://www.facebook.com/`
- Resultaat: `source=facebook, medium=social`

### E-mailnieuwsbrief met UTM-tags
- Landingspagina: `https://yoursite.com/?utm_source=mailchimp&utm_medium=email&utm_term=weekly_digest`
- Referrer: `https://mail.google.com/`
- Resultaat: `source=mailchimp, medium=email, term=weekly_digest`
- De UTM-tags overschrijven de gedetecteerde `source=mail.google.com`.

### Link vanuit een blog
- Referrer: `https://exampleblog.com/article`
- Resultaat: `source=exampleblog.com, medium=referral`

### Bladwijzer of getypte URL
- Geen referrer
- Resultaat: `source=direct, medium=none`

## Technische implementatie (voor ontwikkelaars)

Referentielijsten (zoekmachines, sociale netwerken, videoplatforms, enz.) worden onderhouden als YAML-bestanden in [`pkg/columns/sessioncolumns/smt/`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/). Ontwikkelaars kunnen deze lijsten naar behoefte inspecteren of uitbreiden.

Kernlogica: [`pkg/columns/sessioncolumns/session_smt_source.go`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/session_smt_source.go); andere kolommen nemen de waarden over die door de `source`-kolom uit de cache zijn berekend.

## Attributie

De definities van zoekmachines en sociale netwerken zijn afgeleid van het project [Matomo searchengine-and-social-list](https://github.com/matomo-org/searchengine-and-social-list), beschikbaar onder [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/).
