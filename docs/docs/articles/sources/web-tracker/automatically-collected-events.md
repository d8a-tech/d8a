---
title: Automatically collected events
sidebar_position: 2
---

The d8a web tracker automatically collects certain events when you install it on your site. This behavior mimics the [GA4 automatically collected events](https://support.google.com/analytics/answer/9234069?hl=en), though some events are not currently supported by the d8a web tracker (specifically mobile-app-only events) or are calculated on the backend due to d8a's full session scope support.

## Supported events

| Event                            | Automatically triggered...                                                        | Parameters                                                                                                       | d8a Web Tracker Support                                                                        |
| :------------------------------- | :-------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------- |
| `click`<br/>(web)                | Each time a user clicks a link that leads away from the current domain.           | `link_classes`, `link_domain`, `link_id`, `link_url`, `outbound`                                                 | **Yes** (Default)                                                                              |
| `file_download`<br/>(web)        | When a user clicks a link leading to a file (pdf, zip, etc.).                     | `file_extension`, `file_name`, `link_classes`, `link_id`, `link_text`, `link_url`                                | **Yes** (Default)                                                                              |
| `first_visit`<br/>(web, app)     | The first time a user visits a website or launches an app.                        | `page_location`, `page_referrer`, `page_title`                                                                   | **Backend derived**<br/>(No event needed)                                                      |
| `form_start`<br/>(web)           | The first time a user interacts with a form in a session.                         | `form_id`, `form_name`, `form_destination`                                                                       | **No**<br/>(Better implemented via Tag Manager for precise control)                            |
| `form_submit`<br/>(web)          | When the user submits a form.                                                     | `form_id`, `form_name`, `form_destination`, `form_submit_text`                                                   | **No**<br/>(Better implemented via Tag Manager for precise control)                            |
| `page_view`<br/>(web)            | Each time the page loads.                                                         | `page_location`, `page_referrer`, `page_title`                                                                   | **Yes** (Default)<br/>(SPA page views: Better implemented via Tag Manager for precise control) |
| `scroll`<br/>(web)               | The first time a user reaches the bottom of each page.                            | `engagement_time_msec`                                                                                           | **No**<br/>(Better implemented via Tag Manager for precise control)                            |
| `session_start`<br/>(web, app)   | When a user engages the app or website.                                           | `page_location`, `page_referrer`, `page_title`                                                                   | **Backend derived**<br/>(No event needed)                                                      |
| `user_engagement`<br/>(web, app) | When the app is in the foreground or webpage is in focus for at least one second. | `engagement_time_msec` (`_et`)                                                                                   | **Yes** (Default)                                                                              |
| `view_search_results`<br/>(web)  | Each time a user performs a site search (detected via URL query params).          | `search_term`                                                                                                    | **Yes** (Default)                                                                              |
| `video_start`<br/>(web)          | When a video starts playing.                                                      | `video_current_time`, `video_duration`, `video_percent`, `video_provider`, `video_title`, `video_url`, `visible` | **No**<br/>(Better implemented via Tag Manager for precise control)                            |
| `video_progress`<br/>(web)       | When the video progresses past 10%, 25%, 50%, and 75% duration.                   | `video_current_time`, `video_duration`, `video_percent`, `video_provider`, `video_title`, `video_url`, `visible` | **No**<br/>(Better implemented via Tag Manager for precise control)                            |
| `video_complete`<br/>(web)       | When the video ends.                                                              | `video_current_time`, `video_duration`, `video_percent`, `video_provider`, `video_title`, `video_url`, `visible` | **No**<br/>(Better implemented via Tag Manager for precise control)                            |

## App-only events

The d8a **web tracker** runs only in browser environments and does not support mobile-app-specific events. Support for these events will be added via the Measurement Protocol in the future (see [Issue #241](https://github.com/d8a-tech/d8a/issues/241)).

These include:

- `ad_click`, `ad_exposure`, `ad_impression`, `ad_query`, `ad_reward`, `adunit_exposure`
- `app_clear_data`, `app_exception`, `app_remove`, `app_store_refund`, `app_store_subscription_*`, `app_update`
- `error`, `first_open`, `in_app_purchase`
- `firebase_*`, `dynamic_link_*`
- `notification_*`
- `os_update`
- `screen_view`
