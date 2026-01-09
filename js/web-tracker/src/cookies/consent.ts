export type ConsentStateLike = { analytics_storage?: string };

export function canWriteAnalyticsCookies(consentState: ConsentStateLike) {
  const s = consentState || {};
  // Requirement: if analytics_storage explicitly denied, do not write cookies.
  return String(s.analytics_storage || "").toLowerCase() !== "denied";
}
