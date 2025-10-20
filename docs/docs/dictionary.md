---
sidebar_position: 1
---

# Dictionary

## General

| Term | Description |
| :---- | :---- |
| **Property** | A property is an entity the tracking engine is tracking. It is a website, app, or other digital property. |
| **Hit** | A hit is a single request to the tracking server. Hits are raw and yield no analytical value, after they're processed into events by the tracking pipeline, they can be used for analytics. |
| **Event** | An event is a single action that occurs on the property. Its relation to hit is 1-1. It is used to track the user's activity on the property. |
| **User** | A user is a person who visits the property. It is used to track the user's activity on the property. |

## Tracking pipeline

| Term | Description |
| :---- | :---- |
| **Proto-session** | A set of loosely connected Hits, that may in the future form one or more sessions. The hits are glued together by the identifiers (Client ID, SS). |


## Identifiers
| Identifier type | GA4-compatible ([measurement protocol](https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference?client_type=gtag#payload)) | Matomo-compatible ([matomo.php](https://developer.matomo.org/api-reference/tracking-api)) |
| :---- | :---- | :---- |
| **Client ID** - the ID assigned by the tracking engine to each device/browser combination, persisted in a cookie | `client_id`, `user_pseudo_id` | `_id` |
| **User ID** - the ID used in an internal user system, for example email, available after the login. The customer must manually set it as a tracking parameter, it's not automatically determined by the tracker | `user_id` | `uid` |
| Session stamp (**SS**) - hash calculated on the backend, from the incoming request elements - IP, User Agent, etc | none, calculated on the backend | none, calculated on the backend |
| Client-assigned session ID (**CASI**) - the session id as explicitly set by the client in the tracking request | `session_id` | none |
