# Traffic attribution

This article explains how d8a determines where your traffic comes from by classifying each session with **Source**, **Medium**, and **Term**.

## What are Source, Medium, and Term?

- **Source**: The origin of the traffic (e.g., `google`, `facebook`, `vimeo`, `direct`).
- **Medium**: The marketing channel (e.g., `organic`, `cpc`, `social`, `email`, `referral`).
- **Term**: The search keyword or campaign term (when available from search engines or UTM tags).

These attributes follow standard analytics conventions and help you understand your traffic composition.

## How detection works

Attribution is calculated once per session, analyzing the first event (typically the landing page view). The system examines:

- The landing page URL (with all query parameters intact).
- The HTTP referrer (the page the visitor came from).
- UTM tracking parameters (`utm_source`, `utm_medium`, `utm_term`).
- Ad network click identifiers (`gclid`, `fbclid`, `msclkid`, etc.).

Detection follows a priority order. The first matching rule determines the initial source/medium/term, then UTM parameters override individual fields if present.

## Detection rules (in priority order)

### 1. Paid advertising click IDs

Sessions with landing page URLs containing specific query parameters are immediately classified as paid traffic (medium=cpc). Examples:

| Parameter | Source | Medium |
|:----------|:-------|:-------|
| `gclid`, `gbraid`, `wbraid` | google | cpc |
| `msclkid` | bing | cpc |
| `fbclid` | facebook | cpc |

A full list can be found in the source code: [`pkg/columns/sessioncolumns/session_smt_source.go`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/session_smt_source.go).

### 2. Video platforms

Referrers from video hosting sites are classified as `medium=video`. Sources include YouTube, Vimeo, Dailymotion, Twitch, and others (full list in [`pkg/columns/sessioncolumns/smt/video.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/video.yaml)).

### 3. Email providers

Referrers from webmail interfaces are classified as `medium=email`. This includes Gmail, Outlook, Yahoo Mail, ProtonMail, and others (full list in [`pkg/columns/sessioncolumns/smt/emails.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/emails.yaml)).

Additionally, any referrer containing `mail.` in the hostname (e.g., `mail.company.com`) is automatically treated as email with the normalized domain as the source.

### 4. Social media

Referrers from social networks are classified as `medium=social`. This includes Facebook, Instagram, Twitter/X, LinkedIn, Reddit, TikTok, Pinterest, and many others (full list in [`pkg/columns/sessioncolumns/smt/socials.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/socials.yaml)).

### 5. AI assistants and search

Referrers from AI chat interfaces are classified as `medium=ai`. This includes ChatGPT, Claude, Perplexity, and others (full list in [`pkg/columns/sessioncolumns/smt/ai.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/ai.yaml)).

### 6. Search engines

Referrers from search engines are classified as `medium=organic`. This includes Google, Bing, DuckDuckGo, Yahoo, Baidu, Yandex, and hundreds of regional/specialized engines (full list in [`pkg/columns/sessioncolumns/smt/searchengines.yaml`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/searchengines.yaml)).

The system attempts to extract the search term from the referrer's query parameters (e.g., `q`, `query`, `p`). However, most modern browsers strip query parameters from referrers for privacy, so terms are rarely available unless explicitly passed via UTM tags.

### 7. Generic referral

Any external referrer (from a different domain than your site) that doesn't match the above categories is classified as `medium=referral`, with the normalized domain as the source.

### 8. Direct traffic

If no referrer exists and no paid click IDs are present, traffic is classified as `source=direct`, `medium=none`. This includes:

- Typing the URL directly into the browser
- Clicking bookmarks
- Links from native mobile apps
- Links from documents (PDFs, Office files)
- Secure (HTTPS) to non-secure (HTTP) transitions that strip referrers

## UTM parameter overrides

You can manually control attribution by adding UTM tags to your URLs:

```
https://yoursite.com/?utm_source=newsletter&utm_medium=email&utm_term=spring_sale
```

UTM parameters always override the automatically detected values:

- `utm_source` → overrides Source
- `utm_medium` → overrides Medium  
- `utm_term` → overrides Term

Each parameter is applied independently. If you only provide `utm_source`, the detected medium and term remain unchanged.

**Example**: A visitor clicks a YouTube video description link to `yoursite.com/?utm_source=youtube_channel&utm_medium=video_description`. The referrer would normally detect `source=youtube, medium=video`, but the UTM tags override this to `source=youtube_channel, medium=video_description`.

## Common scenarios

### Organic search from Google
- Referrer: `https://www.google.com/`
- Result: `source=google, medium=organic`
- Note: Search term usually unavailable due to referrer privacy.

### Google Ads click
- Landing page: `https://yoursite.com/?gclid=ABC123XYZ`
- Result: `source=google, medium=cpc`
- The `gclid` triggers immediate paid classification.

### Facebook organic post
- Referrer: `https://www.facebook.com/`
- Result: `source=facebook, medium=social`

### Email newsletter with UTM tags
- Landing page: `https://yoursite.com/?utm_source=mailchimp&utm_medium=email&utm_term=weekly_digest`
- Referrer: `https://mail.google.com/`
- Result: `source=mailchimp, medium=email, term=weekly_digest`
- UTM tags override the detected `source=mail.google.com`.

### Link from a blog
- Referrer: `https://exampleblog.com/article`
- Result: `source=exampleblog.com, medium=referral`

### Bookmark or typed URL
- No referrer
- Result: `source=direct, medium=none`

## Technical implementation (for developers)

Reference lists (search engines, social networks, video platforms, etc.) are maintained as YAML files in [`pkg/columns/sessioncolumns/smt/`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/smt/). Developers can inspect or extend these lists as needed.

Core logic: [`pkg/columns/sessioncolumns/session_smt_source.go`](https://github.com/d8a-tech/d8a/tree/master/pkg/columns/sessioncolumns/session_smt_source.go), other columns take values computed by `source` column from cache.

## Attribution

Search engine and social network definitions are derived from the [Matomo searchengine-and-social-list](https://github.com/matomo-org/searchengine-and-social-list) project, available under [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/).