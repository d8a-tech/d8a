import { buildGa4CollectQueryParams } from "../ga4/gtag_mapper.ts";
import { resolveTrackingUrl } from "../utils/endpoint.ts";
import { getBrowserContext } from "./browser_context.ts";
import { sendWithRetries } from "../transport/send.ts";
import { createCookieJar } from "../cookies/cookie_jar.ts";
import { ensureClientId } from "../cookies/identity.ts";
import { buildD8aClientCookieName } from "../cookies/d8a_cookies.ts";
import { fetchUach, getUachCached, uachToParams } from "./uach.ts";
import { buildGcd, buildGcs } from "../ga4/consent_wire.ts";
import { getOrCreateAnonCid } from "./anon_cid.ts";
import { createEngagementTimer } from "./engagement_timer.ts";
import { getPropertyConfig, getPropertyIds } from "./config_resolver.ts";
import { getConsentParts, getEffectiveConsent } from "./consent_resolver.ts";
import { createDebugLogger } from "./debug_logger.ts";
import { resolveBool, resolveString } from "./param_precedence.ts";
import { resolveCookieSettings } from "../cookies/cookie_settings.ts";
import { updateAndWriteSharedSession } from "../cookies/session_manager.ts";
import { attachD8aCollectMetadata } from "../utils/collect_metadata.ts";
import { isRecord } from "../utils/is_record.ts";
import type { BrowserContext, PropertyConfig, RuntimeState, WindowLike } from "../types.ts";

function normalizeSendTo(v: unknown) {
  if (typeof v === "string" && v.trim()) return [v.trim()];
  if (Array.isArray(v))
    return v.filter((x) => typeof x === "string" && x.trim()).map((x) => String(x).trim());
  return null;
}

const SET_PARAMS_EXCLUDED_FROM_EVENT_MERGE = new Set([
  "send_to",
  "user_id",
  "client_id",
  "campaign_id",
  "campaign_source",
  "campaign_medium",
  "campaign_name",
  "campaign_term",
  "campaign_content",
  "page_location",
  "page_title",
  "page_referrer",
  "content_group",
  "ignore_referrer",
  "language",
  "screen_resolution",
  "cookie_domain",
  "cookie_expires",
  "cookie_flags",
  "cookie_path",
  "cookie_prefix",
  "cookie_update",
  "send_page_view",
  "session_engagement_time_sec",
  "site_search_enabled",
  "site_search_query_params",
  "outbound_clicks_enabled",
  "outbound_exclude_domains",
  "file_downloads_enabled",
  "file_download_extensions",
  "linker",
]);

function getGlobalCustomEventParams(setParams: Record<string, unknown>) {
  const merged: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(setParams || {})) {
    if (SET_PARAMS_EXCLUDED_FROM_EVENT_MERGE.has(key)) continue;
    merged[key] = value;
  }
  return merged;
}

/** What a wire params resolver learns about the hit it is resolved for. */
export type WireParamsContext = {
  /** Effective `analytics_storage` is denied for this hit (the consent its `gcs` reports). */
  analyticsStorageDenied: boolean;
};

/**
 * Options for events enqueued by tracker extensions.
 * - `wireParams`: sent as-is (no `ep.`/`epn.` prefix), after the standard collect params. A
 *   function is called when the event is sent, with the consent that hit is sent with; it
 *   returns the params, or null to drop the event (as does throwing).
 * - `passive`: the event does not take the accumulated engagement time (`_et`).
 */
export type EnqueueEventOptions = {
  wireParams?: Record<string, string> | ((ctx: WireParamsContext) => Record<string, string> | null);
  passive?: boolean;
};

function resolveWireParams(
  wireParams: EnqueueEventOptions["wireParams"],
  ctx: WireParamsContext,
): Record<string, string> | null {
  if (typeof wireParams !== "function") return wireParams || {};
  try {
    return wireParams(ctx);
  } catch {
    return null;
  }
}

// Hits whose query string is longer than this are sent in the POST body instead of the URL.
const MAX_QUERY_LENGTH = 2000;

export function createDispatcher({
  windowRef,
  getState,
}: {
  windowRef: WindowLike;
  getState: () => RuntimeState;
}) {
  const w = windowRef;
  if (!w) throw new Error("createDispatcher: windowRef is required");

  const queue: Array<{
    name: string;
    params: Record<string, unknown>;
    opts?: EnqueueEventOptions;
  }> = [];
  let timerId: number | null = null;
  let lifecycleAttached = false;
  const jar = createCookieJar({ windowRef: w });

  // Kick off UA-CH fetching early, but don't block sends.
  fetchUach(w);

  // Engagement timer for `_et`.
  const engagementTimer = createEngagementTimer({ windowRef: w });
  engagementTimer.start();

  // Avoid emitting user_engagement on every tab switch. Only emit on
  // visibilitychange(hidden) when we have at least N ms of engaged time to flush.
  // Default 10s (configurable via `session_engagement_time_sec`).
  const USER_ENGAGEMENT_VISIBILITY_MIN_MS = 10_000;

  function getPrimaryConfig() {
    const ids = getPropertyIds(getState);
    const primaryId = ids[0] || "";
    return primaryId ? getPropertyConfig(getState, primaryId) : {};
  }

  function isDebugEnabled({ config, setParams }: { config: unknown; setParams: unknown }) {
    const cfg = isRecord(config) ? config : {};
    const sp = isRecord(setParams) ? setParams : {};
    return cfg.debug_mode === true || sp.debug_mode === true;
  }

  function isHttps() {
    try {
      return String(w?.location?.protocol || "").toLowerCase() === "https:";
    } catch {
      return false;
    }
  }

  function ensurePageLoadMs() {
    const s = getState();
    if (!s) return null;
    if (s.pageLoadMs != null) return s.pageLoadMs;
    if (s.jsDate instanceof Date && !Number.isNaN(s.jsDate.getTime())) {
      s.pageLoadMs = s.jsDate.getTime();
      return s.pageLoadMs;
    }
    s.pageLoadMs = Date.now();
    return s.pageLoadMs;
  }

  function enqueueEvent(name: string, params: Record<string, unknown>, opts?: EnqueueEventOptions) {
    queue.push({ name, params: params || {}, opts });

    const cfg = getPrimaryConfig();
    const maxBatchSize = Number(cfg?.max_batch_size ?? 25);
    if (queue.length >= maxBatchSize) {
      flush({ useBeacon: false });
      return;
    }

    ensureTimer();
  }

  function ensureTimer() {
    if (timerId != null) return;
    const cfg = getPrimaryConfig();
    const interval = Number(cfg?.flush_interval_ms ?? 1000);
    timerId = w.setTimeout(() => {
      timerId = null;
      flush({ useBeacon: false });
    }, interval);
  }

  // `concurrent`: start every request before awaiting any (used on page exit, where only
  // requests started synchronously survive). Otherwise events go out one after another,
  // so the collector receives them in order.
  async function flush({
    useBeacon,
    concurrent = false,
  }: {
    useBeacon: boolean;
    concurrent?: boolean;
  }) {
    if (timerId != null) {
      w.clearTimeout(timerId);
      timerId = null;
    }
    if (queue.length === 0) return { sent: 0 };

    const propertyIds = getPropertyIds(getState);
    const primaryId = propertyIds[0] || "";
    const cfgPrimary = primaryId ? getPropertyConfig(getState, primaryId) : {};
    const maxBatchSize = Number(cfgPrimary?.max_batch_size ?? 25);
    const batch = queue.splice(0, maxBatchSize);

    const consent = getEffectiveConsent(getState);
    const consentParts = getConsentParts(getState);
    const sessionTimeoutMsRaw = cfgPrimary["session_timeout_ms"];
    const sessionTimeoutMs =
      typeof sessionTimeoutMsRaw === "number" ? sessionTimeoutMsRaw : 30 * 60 * 1000;
    const state = getState();
    const pageLoadMs = ensurePageLoadMs();

    const analyticsStorage = String(consent?.analytics_storage || "").toLowerCase();
    const analyticsDenied = analyticsStorage === "denied";
    const setParams = state?.set && typeof state.set === "object" ? state.set : {};
    const globalCustomEventParams = getGlobalCustomEventParams(setParams);
    const https = isHttps();
    const debugPrimary = isDebugEnabled({ config: cfgPrimary, setParams });
    const engagedThresholdSecRaw =
      cfgPrimary?.session_engagement_time_sec ?? setParams?.session_engagement_time_sec ?? 10;
    const engagedThresholdSec = Number.isFinite(Number(engagedThresholdSecRaw))
      ? Number(engagedThresholdSecRaw)
      : 10;
    const engagedThresholdMs = Math.max(0, Math.floor(engagedThresholdSec * 1000));

    const logger = createDebugLogger({ enabled: debugPrimary });
    logger.log("[d8a] flush", {
      href: String(w?.location?.href || ""),
      propertyIds,
      analyticsStorage: analyticsStorage || "(unset)",
      analyticsDenied,
      cookieHeader: String(w?.document?.cookie || ""),
    });

    function resolveCookiesFor(propertyCfg: PropertyConfig) {
      return resolveCookieSettings({
        propertyCfg,
        setParams,
        https,
        state,
        isDebugEnabled,
      });
    }

    let sent = 0;
    const pending: Array<Promise<unknown>> = [];
    for (const ev of batch) {
      const rawParams = isRecord(ev.params) ? ev.params : {};
      const sendTo = normalizeSendTo(rawParams["send_to"]);
      const destinations = sendTo ? sendTo : propertyIds;
      if (!destinations || destinations.length === 0) continue;

      // Extension params are resolved with the consent this batch is sent with; null drops the
      // event before it takes engagement time or a hit number.
      const wireParams = resolveWireParams(ev.opts?.wireParams, {
        analyticsStorageDenied: analyticsDenied,
      });
      if (wireParams === null) continue;

      // Merge global set() custom params into each event; event payload wins.
      // This mirrors gtag semantics for custom params without changing
      // precedence handling for reserved override keys.
      const eventParams: Record<string, unknown> = { ...globalCustomEventParams, ...rawParams };
      // Remove routing metadata from params so it doesn't end up in `ep.*`.
      delete eventParams["send_to"];

      // Engagement time `_et`: active+visible+focused time since last reset.
      // We compute this once per event and reuse it:
      // - send as `_et`
      // - accumulate to flip session engagement (`seg`) after N seconds
      const engagementTimeMs = ev.opts?.passive ? 0 : engagementTimer.getAndReset();

      // Shared session update (one per event) to keep per-property cookies identical.
      if (!analyticsDenied && state) {
        updateAndWriteSharedSession({
          state,
          destinations,
          jar,
          getPropertyConfig: (pid: string) => getPropertyConfig(getState, pid),
          resolveCookieSettings: resolveCookiesFor,
          sessionTimeoutMs,
          engagementTimeMs,
          engagedThresholdMs,
          nowMs: Date.now(),
          log: logger.enabled ? logger.log : null,
        });
      }

      // Maintain GA hit counter `_s` per session (shared).
      if (state) state.hitCounter = (state.hitCounter || 0) + 1;

      const sendPromises: Array<Promise<unknown>> = [];
      for (const pid of destinations) {
        const pcfg = getPropertyConfig(getState, pid);
        const collectBase = resolveTrackingUrl(pcfg);
        const cs = resolveCookiesFor(pcfg);

        const resolvedUserId = resolveString({
          key: "user_id",
          eventParams,
          config: pcfg,
          setParams,
        });
        const resolvedClientId = resolveString({
          key: "client_id",
          eventParams,
          config: pcfg,
          setParams,
        });
        const resolvedDebugMode =
          resolveBool({ key: "debug_mode", eventParams, config: pcfg, setParams }) === true;

        const campaign = {
          campaign_id: resolveString({ key: "campaign_id", eventParams, config: pcfg, setParams }),
          campaign_source: resolveString({
            key: "campaign_source",
            eventParams,
            config: pcfg,
            setParams,
          }),
          campaign_medium: resolveString({
            key: "campaign_medium",
            eventParams,
            config: pcfg,
            setParams,
          }),
          campaign_name: resolveString({
            key: "campaign_name",
            eventParams,
            config: pcfg,
            setParams,
          }),
          campaign_term: resolveString({
            key: "campaign_term",
            eventParams,
            config: pcfg,
            setParams,
          }),
          campaign_content: resolveString({
            key: "campaign_content",
            eventParams,
            config: pcfg,
            setParams,
          }),
        };
        const contentGroup = resolveString({
          key: "content_group",
          eventParams,
          config: pcfg,
          setParams,
        });

        const ignoreReferrer =
          resolveBool({ key: "ignore_referrer", eventParams, config: pcfg, setParams }) === true;

        const browser: BrowserContext & Record<string, unknown> = { ...getBrowserContext(w) };
        const pageLocation = resolveString({
          key: "page_location",
          eventParams,
          config: pcfg,
          setParams,
        });
        const pageTitle = resolveString({
          key: "page_title",
          eventParams,
          config: pcfg,
          setParams,
        });
        const pageReferrer = resolveString({
          key: "page_referrer",
          eventParams,
          config: pcfg,
          setParams,
        });
        const language = resolveString({ key: "language", eventParams, config: pcfg, setParams });
        const screenResolution = resolveString({
          key: "screen_resolution",
          eventParams,
          config: pcfg,
          setParams,
        });

        if (pageLocation) browser.dl = pageLocation;
        if (pageTitle) browser.dt = pageTitle;
        if (pageReferrer != null) browser.dr = pageReferrer;
        if (language) browser.ul = language;
        if (screenResolution) browser.sr = screenResolution;

        // Ensure client cookie exists for this property cookie namespace (if allowed).
        if (!analyticsDenied) {
          if (cs.debug) {
            const clientCookieName = buildD8aClientCookieName(cs.cookiePrefix);
            const clientCookieBefore = jar.get(clientCookieName);
            const clientRes = ensureClientId({
              jar,
              consent,
              cookieDomain: cs.cookieDomain,
              cookiePrefix: cs.cookiePrefix,
              cookiePath: cs.cookiePath,
              cookieSameSite: cs.cookieSameSite,
              cookieSecure: cs.cookieSecure,
              cookieMaxAgeSeconds: cs.cookieMaxAgeSeconds ?? undefined,
              cookieUpdate: cs.cookieUpdate,
              forceCookieAttrsWrite: cs.forceCookieAttrsWrite,
            });

            const clientCookieAction = !clientRes?.wrote
              ? "no write"
              : clientCookieBefore
                ? "attrs update"
                : "create";
            logger.log(`[d8a] set client cookie (${clientCookieAction})`, {
              pid,
              cookiePrefix: cs.cookiePrefix,
              result: clientRes,
              cookieName: clientCookieName,
              cookieBefore: clientCookieBefore,
              cookieReadBack: jar.get(clientCookieName),
              documentCookie: String(w?.document?.cookie || ""),
            });
          } else {
            ensureClientId({
              jar,
              consent,
              cookieDomain: cs.cookieDomain,
              cookiePrefix: cs.cookiePrefix,
              cookiePath: cs.cookiePath,
              cookieSameSite: cs.cookieSameSite,
              cookieSecure: cs.cookieSecure,
              cookieMaxAgeSeconds: cs.cookieMaxAgeSeconds ?? undefined,
              cookieUpdate: cs.cookieUpdate,
              forceCookieAttrsWrite: cs.forceCookieAttrsWrite,
            });
          }
        }

        const q = buildGa4CollectQueryParams({
          propertyId: pid,
          eventName: ev.name,
          eventParams,
          cookieHeader: analyticsDenied ? "" : String(w.document?.cookie || ""),
          clientId:
            resolvedClientId ||
            (analyticsDenied ? getOrCreateAnonCid({ windowRef: w, state }) : null),
          userId: resolvedUserId,
          cookiePrefix: cs.cookiePrefix,
          browser,
          ignoreReferrer,
          pageLoadMs: pageLoadMs != null ? Number(pageLoadMs) : null,
          hitCounter: state?.hitCounter ?? null,
          engagementTimeMs: engagementTimeMs != null ? Number(engagementTimeMs) : null,
          uachParams: uachToParams(getUachCached(w)),
          campaign,
          contentGroup: contentGroup || null,
          consentParams: {
            gcs: buildGcs({ consent: consentParts.consent }),
            gcd: buildGcd({
              consentDefault: consentParts.consentDefault,
              consentUpdate: consentParts.consentUpdate,
            }),
          },
          debugMode: resolvedDebugMode,
        });
        attachD8aCollectMetadata(q);
        for (const [k, v] of Object.entries(wireParams)) q.set(k, v);
        const query = q.toString();
        if (query.length > MAX_QUERY_LENGTH) {
          sendPromises.push(
            sendWithRetries({ url: collectBase, body: query, windowRef: w, useBeacon }),
          );
        } else {
          sendPromises.push(
            sendWithRetries({ url: `${collectBase}?${query}`, windowRef: w, useBeacon }),
          );
        }
      }
      sent += sendPromises.length;
      if (concurrent) pending.push(...sendPromises);
      else if (sendPromises.length > 0) await Promise.all(sendPromises);
    }
    if (pending.length > 0) await Promise.all(pending);

    // If there are more events pending, schedule another flush.
    if (queue.length > 0) ensureTimer();
    return { sent };
  }

  function flushNow({
    useBeacon = false,
    concurrent = false,
  }: { useBeacon?: boolean; concurrent?: boolean } = {}) {
    return flush({ useBeacon: useBeacon === true, concurrent: concurrent === true });
  }

  function attachLifecycleFlush() {
    if (!w?.addEventListener) return;
    if (lifecycleAttached) return;
    lifecycleAttached = true;

    w.addEventListener("pagehide", () => {
      // Ensure user_engagement carries the accumulated engagement time.
      try {
        const ids = getPropertyIds(getState);
        if (ids.length > 0 && engagementTimer.peek() > 0) {
          queue.unshift({ name: "user_engagement", params: {} });
        }
      } catch {
        // ignore
      }
      flush({ useBeacon: false, concurrent: true });
    });

    if (w.document?.addEventListener) {
      w.document.addEventListener("visibilitychange", () => {
        if (w.document.hidden) {
          try {
            const ids = getPropertyIds(getState);
            if (ids.length > 0 && engagementTimer.peek() >= USER_ENGAGEMENT_VISIBILITY_MIN_MS) {
              queue.unshift({ name: "user_engagement", params: {} });
            }
          } catch {
            // ignore
          }
          // Always use fetch(keepalive) on lifecycle flushes (no sendBeacon/ping).
          flush({ useBeacon: false, concurrent: true });
        }
      });
    }
  }

  return { enqueueEvent, flush, flushNow, attachLifecycleFlush };
}
