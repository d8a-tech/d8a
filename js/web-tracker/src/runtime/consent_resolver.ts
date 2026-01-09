import type { ConsentState, RuntimeState } from "../types.ts";

export function getEffectiveConsent(getState: () => RuntimeState): ConsentState {
  const s = typeof getState === "function" ? getState() : null;
  const g = s?.consentGtag && typeof s.consentGtag === "object" ? s.consentGtag : {};
  if (g && Object.keys(g).length > 0) return g;
  return s?.consent && typeof s.consent === "object" ? s.consent : {};
}

export function getConsentParts(getState: () => RuntimeState): {
  consent: ConsentState;
  consentDefault: ConsentState;
  consentUpdate: ConsentState;
} {
  const s = typeof getState === "function" ? getState() : null;
  const gConsent = s?.consentGtag && typeof s.consentGtag === "object" ? s.consentGtag : {};
  const gDefault =
    s?.consentDefaultGtag && typeof s.consentDefaultGtag === "object" ? s.consentDefaultGtag : {};
  const gUpdate =
    s?.consentUpdateGtag && typeof s.consentUpdateGtag === "object" ? s.consentUpdateGtag : {};
  const hasGtag =
    (gConsent && Object.keys(gConsent).length > 0) ||
    (gDefault && Object.keys(gDefault).length > 0) ||
    (gUpdate && Object.keys(gUpdate).length > 0);

  if (hasGtag) {
    return { consent: gConsent, consentDefault: gDefault, consentUpdate: gUpdate };
  }

  return {
    consent: s?.consent && typeof s.consent === "object" ? s.consent : {},
    consentDefault:
      s?.consentDefault && typeof s.consentDefault === "object" ? s.consentDefault : {},
    consentUpdate: s?.consentUpdate && typeof s.consentUpdate === "object" ? s.consentUpdate : {},
  };
}
