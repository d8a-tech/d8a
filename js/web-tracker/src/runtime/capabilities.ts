/**
 * Features this runtime offers to extensions bundled with the tracker (e.g. a cloud bundle that
 * adds its own events). A page may already run an older tracker, and `installD8a()` then
 * returns that runtime, so extensions check these flags before relying on a feature. Older
 * runtimes have no `capabilities` at all.
 */
export const EXTENSION_CAPABILITIES = Object.freeze({
  /** `dispatcher.enqueueEvent(name, params, { wireParams })` sends params as-is, long hits in the body. */
  wireParams: true,
  /** `dispatcher.enqueueEvent(name, params, { passive: true })` takes no engagement time. */
  passive: true,
  /** `dispatcher.flushNow({ concurrent: true })` starts every queued request at once. */
  concurrentFlush: true,
  /** `consent.analyticsStorage()` and `consent.onAnalyticsStorageChange(cb)`. */
  consentHook: true,
} as const);

export type ExtensionCapabilities = typeof EXTENSION_CAPABILITIES;
