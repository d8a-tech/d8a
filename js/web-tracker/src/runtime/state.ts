import type { RuntimeState } from "../types.ts";

export function createRuntimeState(): RuntimeState {
  const state: RuntimeState = {
    jsDate: null,
    pageLoadMs: null, // maps to GA `_p`
    // Multi-property support:
    // - propertyIds preserves insertion order for default fan-out routing
    // - propertyConfigs holds per-property config objects as passed to `config()`
    // - primaryPropertyId is the first configured property
    propertyIds: [],
    propertyConfigs: {},
    primaryPropertyId: "",
    consent: {}, // effective consent state (default + update merged)
    consentDefault: {},
    consentUpdate: {},

    // If gtag/GTM is present and pushes consent commands into `window.dataLayer`,
    // we mirror those here and prefer them over d8a's own consent commands.
    consentGtag: {},
    consentDefaultGtag: {},
    consentUpdateGtag: {},

    userId: null,
    set: {},
    linker: { domains: [] },
    incomingDl: null,
    events: [],
    __onEvent: null,
    __onConfig: null,
    __onSet: null,

    // Request-scoped counters/timing
    hitCounter: 0, // maps to `_s`
    // `_et` is computed from an engagement timer (not wall-clock delta),
    // but we still keep session-level engagement accumulation to flip `g` in session context cookie.
    sessionEngagementMs: 0,
    sessionEngaged: false,

    // Cookieless (analytics_storage denied) ephemeral identity.
    // We keep this in-memory for the lifetime of the page so SPAs can have a
    // stable identifier without reading/writing cookies.
    anonCid: null,

    // Tracks the last applied cookie attribute signature so we can apply
    // security-related cookie attribute changes even when cookie_update=false.
    // Note: when cookie_update=false we do not refresh/update cookie values; only attributes may be rewritten.
    cookieAttrsSig: null,

    // Multi-property shared session state:
    // We compute session tokens once per event and write the same serialized
    // value to each destination property cookie.
    sharedSessionTokens: null,
    sharedSessionValue: null,

    // Enhanced measurement
    emInstalled: false,
    emSiteSearchFired: false,

    // Consent transition tracking (for best-effort consent-update pings).
    // We track the *effective* Consent Mode value (preferring mirrored gtag consent if present).
    __lastEffectiveAnalyticsStorage: undefined,
  };
  return state;
}
