---
sidebar_position: 2
---

# Glossary

## General

| Term             | Description                                                                                                                                                                                 |
| :--------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Property**     | A property is an entity the tracking engine is tracking. It is a website, app, or other digital property.                                                                                   |
| **Hit**          | A hit is a single request to the tracking server. Hits are raw and yield no analytical value, after they're processed into events by the tracking pipeline, they can be used for analytics. |
| **Event**        | An event is a single action that occurs on the property. Its relation to hit is 1-1. It is used to track the user's activity on the property.                                               |
| **User**         | A user is a person who visits the property. It is used to track the user's activity on the property.                                                                                        |
| **Tracking URL** | The endpoint that receives tracking data, e.g., `https://global.t.d8a.tech/<property_id>/g/collect` for d8a Cloud and GA4/gtag protocol.                                                    |
| **Web tracker**  | Client-side tracking library that provides a GA4 gtag-style API and sends GA4 gtag-compatible requests to a d8a collector.                                                                  |

## Tracking pipeline

| Term              | Description                                                                                                                                        |
| :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Proto-session** | A set of loosely connected Hits, that may in the future form one or more sessions. The hits are glued together by the identifiers (Client ID, SS). |

## Identifiers

| Identifier type                                                                                                                                                                                                                                                                                       | GA4-compatible (<a href="https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference?client_type=gtag#payload" rel="nofollow noreferrer noopener" target="_blank">measurement protocol</a>) | Matomo-compatible (<a href="https://developer.matomo.org/api-reference/tracking-api" rel="nofollow noreferrer noopener" target="_blank">matomo.php</a>) |
| :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Client ID** – The ID assigned by the tracking engine to each device/browser combination, persisted in a cookie                                                                                                                                                                                      | `client_id`, `user_pseudo_id`                                                                                                                                                                                       | `_id`                                                                                                                                                   |
| **User ID** – The ID used in an internal user system, for example email, available after the login. The customer must manually set it as a tracking parameter; it's not automatically determined by the tracker                                                                                       | `user_id`                                                                                                                                                                                                           | `uid`                                                                                                                                                   |
| **Session stamp (SS)** – Hash calculated on the backend from the incoming request elements. Currently it includes: IP address, Property ID and a subset of http request headers. Inspect [the code for more details](https://github.com/d8a-tech/d8a/blob/master/pkg/protosessions/isolation.go#L16). | none, calculated on the backend                                                                                                                                                                                     | none, calculated on the backend                                                                                                                         |
| **Client-assigned session ID (CASI)** – The session ID explicitly set by the client in the tracking request. This value is not used for backend sessionization; it is stored as an additional column for reference.                                                                                   | `session_id`                                                                                                                                                                                                        | none                                                                                                                                                    |

## Cookies

| Term                       | Description                                                                                                                                                                                                                                                        |
| :------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Client ID cookie**       | First-party cookie created by the d8a web tracker to persist the Client ID (used to derive the GA4-style `cid` parameter). Default name is `_d8a` and the value format is `C1.1.<random_31bit_int>.<timestamp_seconds>`.                                           |
| **Session context cookie** | First-party cookie created by the d8a web tracker to persist per-property session state. Default name is `_d8a_<property_id>` and the value is a token list prefixed with `S1.1.`. It includes the session engagement flag (`g`, sent as `seg` to the d8a server). |
