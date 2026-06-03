---
title: Automatisch verzamelde events
sidebar_position: 2
---

De d8a web tracker verzamelt automatisch bepaalde events wanneer je hem op je site installeert. Dit gedrag bootst de [automatisch verzamelde GA4-events](https://support.google.com/analytics/answer/9234069?hl=en) na, hoewel sommige events momenteel niet door de d8a web tracker worden ondersteund (specifiek alleen-mobiele-app-events) of op de backend worden berekend dankzij de volledige session-scope-ondersteuning van d8a.

## Ondersteunde events

| Event                            | Automatisch getriggerd...                                                          | Parameters                                                                                                       | Ondersteuning d8a web tracker                                                                  |
| :------------------------------- | :-------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------- |
| `click`<br/>(web)                | Telkens wanneer een gebruiker op een link klikt die van het huidige domein wegleidt. | `link_classes`, `link_domain`, `link_id`, `link_url`, `outbound`                                                 | **Ja** (Standaard)                                                                             |
| `file_download`<br/>(web)        | Wanneer een gebruiker op een link klikt die naar een bestand leidt (pdf, zip, enz.). | `file_extension`, `file_name`, `link_classes`, `link_id`, `link_text`, `link_url`                                | **Ja** (Standaard)                                                                             |
| `first_visit`<br/>(web, app)     | De eerste keer dat een gebruiker een website bezoekt of een app start.            | `page_location`, `page_referrer`, `page_title`                                                                   | **Afgeleid op backend**<br/>(Geen event nodig)                                                 |
| `form_start`<br/>(web)           | De eerste keer dat een gebruiker in een session met een formulier interacteert.   | `form_id`, `form_name`, `form_destination`                                                                       | **Nee**<br/>(Beter te implementeren via een Tag Manager voor nauwkeurige controle)             |
| `form_submit`<br/>(web)          | Wanneer de gebruiker een formulier verstuurt.                                     | `form_id`, `form_name`, `form_destination`, `form_submit_text`                                                   | **Nee**<br/>(Beter te implementeren via een Tag Manager voor nauwkeurige controle)             |
| `page_view`<br/>(web)            | Telkens wanneer de pagina laadt.                                                  | `page_location`, `page_referrer`, `page_title`                                                                   | **Ja** (Standaard)<br/>(SPA-page views: beter te implementeren via een Tag Manager voor nauwkeurige controle) |
| `scroll`<br/>(web)               | De eerste keer dat een gebruiker de onderkant van elke pagina bereikt.            | `engagement_time_msec`                                                                                           | **Nee**<br/>(Beter te implementeren via een Tag Manager voor nauwkeurige controle)             |
| `session_start`<br/>(web, app)   | Wanneer een gebruiker de app of website engageert.                               | `page_location`, `page_referrer`, `page_title`                                                                   | **Afgeleid op backend**<br/>(Geen event nodig)                                                 |
| `user_engagement`<br/>(web, app) | Wanneer de app op de voorgrond is of de webpagina ten minste één seconde in focus is. | `engagement_time_msec` (`_et`)                                                                                   | **Ja** (Standaard)                                                                             |
| `view_search_results`<br/>(web)  | Telkens wanneer een gebruiker een site search uitvoert (gedetecteerd via URL-queryparameters). | `search_term`                                                                                                    | **Ja** (Standaard)                                                                             |
| `video_start`<br/>(web)          | Wanneer een video begint te spelen.                                              | `video_current_time`, `video_duration`, `video_percent`, `video_provider`, `video_title`, `video_url`, `visible` | **Nee**<br/>(Beter te implementeren via een Tag Manager voor nauwkeurige controle)             |
| `video_progress`<br/>(web)       | Wanneer de video voorbij 10%, 25%, 50% en 75% van de duur komt.                  | `video_current_time`, `video_duration`, `video_percent`, `video_provider`, `video_title`, `video_url`, `visible` | **Nee**<br/>(Beter te implementeren via een Tag Manager voor nauwkeurige controle)             |
| `video_complete`<br/>(web)       | Wanneer de video eindigt.                                                        | `video_current_time`, `video_duration`, `video_percent`, `video_provider`, `video_title`, `video_url`, `visible` | **Nee**<br/>(Beter te implementeren via een Tag Manager voor nauwkeurige controle)             |

## Alleen-app-events

De d8a **web tracker** draait alleen in browseromgevingen en ondersteunt geen mobiele-app-specifieke events. Ondersteuning voor deze events wordt in de toekomst toegevoegd via het Measurement Protocol (zie [Issue #241](https://github.com/d8a-tech/d8a/issues/241)).

Hieronder vallen:

- `ad_click`, `ad_exposure`, `ad_impression`, `ad_query`, `ad_reward`, `adunit_exposure`
- `app_clear_data`, `app_exception`, `app_remove`, `app_store_refund`, `app_store_subscription_*`, `app_update`
- `error`, `first_open`, `in_app_purchase`
- `firebase_*`, `dynamic_link_*`
- `notification_*`
- `os_update`
- `screen_view`
