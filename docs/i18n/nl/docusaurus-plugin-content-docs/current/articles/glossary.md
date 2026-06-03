---
sidebar_position: 2
---

# Begrippenlijst

## Algemeen

| Term             | Beschrijving                                                                                                                                                                                 |
| :--------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Property**     | Een property is een entiteit die de tracking-engine volgt. Het is een website, app of andere digitale property.                                                                              |
| **Hit**          | Een hit is een enkel verzoek aan de trackingserver. Hits zijn ruw en leveren op zichzelf geen analytische waarde op; nadat ze door de tracking-pipeline tot events zijn verwerkt, kunnen ze voor analytics worden gebruikt. |
| **Event**        | Een event is een enkele actie die zich op de property voordoet. De relatie met een hit is 1-op-1. Het wordt gebruikt om de activiteit van de gebruiker op de property te volgen.             |
| **User**         | Een user is een persoon die de property bezoekt. Het wordt gebruikt om de activiteit van de gebruiker op de property te volgen.                                                              |
| **Tracking-URL** | Het endpoint dat trackingdata ontvangt, bijv. `https://global.t.d8a.tech/<property_id>/g/collect` voor d8a Cloud en het GA4/gtag-protocol.                                                   |
| **Web tracker**  | Client-side tracking-library die een GA4 gtag-achtige API biedt en GA4 gtag-compatibele verzoeken naar een d8a-collector stuurt.                                                             |

## Tracking-pipeline

| Term              | Beschrijving                                                                                                                                       |
| :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Proto-session** | Een verzameling losjes verbonden hits die in de toekomst een of meer sessions kunnen vormen. De hits worden aan elkaar gekoppeld via de identifiers (Client ID, SS). |

## Identifiers

| Type identifier                                                                                                                                                                                                                                                                                       | GA4-compatibel (<a href="https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference?client_type=gtag#payload" rel="nofollow noreferrer noopener" target="_blank">measurement protocol</a>) | Matomo-compatibel (<a href="https://developer.matomo.org/api-reference/tracking-api" rel="nofollow noreferrer noopener" target="_blank">matomo.php</a>) |
| :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Client ID** – De ID die de tracking-engine toekent aan elke combinatie van apparaat/browser, opgeslagen in een cookie                                                                                                                                                                               | `client_id`, `user_pseudo_id`                                                                                                                                                                                       | `_id`                                                                                                                                                   |
| **User ID** – De ID die wordt gebruikt in een intern gebruikerssysteem, bijvoorbeeld e-mail, beschikbaar na de login. De klant moet deze handmatig als trackingparameter instellen; hij wordt niet automatisch door de tracker bepaald                                                                | `user_id`                                                                                                                                                                                                           | `uid`                                                                                                                                                   |
| **Session stamp (SS)** – Hash die op de backend wordt berekend uit de elementen van het binnenkomende verzoek. Momenteel omvat dit: IP-adres, Property ID en een subset van de http request headers. Bekijk [de code voor meer details](https://github.com/d8a-tech/d8a/blob/master/pkg/protosessions/isolation.go#L16). | geen, berekend op de backend                                                                                                                                                                                       | geen, berekend op de backend                                                                                                                           |
| **Client-assigned session ID (CASI)** – De session-ID die expliciet door de client in het trackingverzoek wordt ingesteld. Deze waarde wordt niet gebruikt voor sessionization op de backend; hij wordt als extra kolom ter referentie opgeslagen.                                                    | `session_id`                                                                                                                                                                                                        | geen                                                                                                                                                   |

## Cookies

| Term                       | Beschrijving                                                                                                                                                                                                                                                        |
| :------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Client ID-cookie**       | First-party cookie die door de d8a web tracker wordt aangemaakt om de Client ID te bewaren (gebruikt om de GA4-achtige `cid`-parameter af te leiden). De standaardnaam is `_d8a` en het waardeformaat is `C1.1.<random_31bit_int>.<timestamp_seconds>`.            |
| **Session context-cookie** | First-party cookie die door de d8a web tracker wordt aangemaakt om de session-status per property te bewaren. De standaardnaam is `_d8a_<property_id>` en de waarde is een tokenlijst met het voorvoegsel `S1.1.`. Het bevat de session engagement-vlag (`g`, verzonden als `seg` naar de d8a-server). |
