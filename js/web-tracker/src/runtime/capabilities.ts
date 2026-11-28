/**
 * Features this runtime offers to extensions bundled with the tracker (e.g. a cloud bundle that
 * adds its own events). A page may already run an older tracker, and `installD8a()` then
 * returns that runtime, so extensions check these flags before relying on a feature. Older
 * runtimes have no `capabilities` at all.
 */
export const EXTENSION_CAPABILITIES = Object.freeze({
  /** `dispatcher.enqueueEvent(name, params, { wireParams })` sends params as-is, long hits in the body. */
  wireParams: true,
  /**
   * `dispatcher.enqueueEvent(name, params, { annotation: true })` reads the tracker's state
   * without changing it: no engagement time, session activity, hit counter or cookie writes.
   */
  annotation: true,
  /** `dispatcher.flushNow({ concurrent: true })` starts every queued request at once. */
  concurrentFlush: true,
  /** `consent.analyticsStorage()` and `consent.onAnalyticsStorageChange(cb)`. */
  consentHook: true,
  /** `wireParams` may be a function, resolved when the hit is sent with that hit's consent. */
  dispatchTimeWireParams: true,
  /** `config.onPropertyConfigured(cb)` replays configured properties, then reports each `config`. */
  configHook: true,
} as const);

export type ExtensionCapabilities = typeof EXTENSION_CAPABILITIES;
