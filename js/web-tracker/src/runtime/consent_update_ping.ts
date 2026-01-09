import { getEffectiveConsent } from "./consent_resolver.ts";
import type { RuntimeState } from "../types.ts";

function scheduleMicrotask(fn: () => void) {
  if (typeof queueMicrotask === "function") {
    queueMicrotask(fn);
    return;
  }
  // Fallback (older environments)
  Promise.resolve().then(fn);
}

/**
 * Tracks effective `analytics_storage` and emits a best-effort ping when it changes.
 *
 * Behavior:
 * - Only fires if at least one property is configured.
 * - Emits `user_engagement` with `consent_update=1` as an event param.
 * - Uses effective consent (prefers mirrored gtag/GTM consent if present).
 */
export function maybeEmitConsentUpdatePing(getState: () => RuntimeState) {
  const s = typeof getState === "function" ? getState() : null;
  if (!s) return;

  const consent = getEffectiveConsent(getState);
  const next =
    consent && typeof consent.analytics_storage === "string"
      ? consent.analytics_storage
      : undefined;

  const prev = s.__lastEffectiveAnalyticsStorage;
  // First observation initializes consent state; no ping.
  // We also start a one-tick "init window" so immediate default->update (same tick)
  // is treated as initial consent setup rather than a user-visible consent change.
  if (prev === undefined) {
    s.__lastEffectiveAnalyticsStorage = next;
    if (!s.__consentPingInitScheduled) {
      s.__consentPingInitScheduled = true;
      scheduleMicrotask(() => {
        s.__consentPingInitDone = true;
      });
    }
    return;
  }

  if (next === prev) return;
  s.__lastEffectiveAnalyticsStorage = next;

  // Suppress pings during the initialization tick (matches gtag behavior for
  // immediate default+update sequences during boot).
  if (!s.__consentPingInitDone) return;

  // Drop before config: no destinations exist yet.
  if (!Array.isArray(s.propertyIds) || s.propertyIds.length === 0) return;
  if (typeof s.__onEvent !== "function") return;

  s.__onEvent("user_engagement", { consent_update: 1 });
}
