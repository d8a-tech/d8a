import { getEffectiveConsent } from "./consent_resolver.ts";
import type { ConsentValue, RuntimeState } from "../types.ts";

export type ConsentHook = {
  /** Effective `analytics_storage` (mirrored gtag/GTM consent first), or undefined when unset. */
  analyticsStorage(): ConsentValue | undefined;
  /**
   * Called whenever the effective `analytics_storage` changes, including its first value.
   * Returns a function that removes the listener.
   */
  onAnalyticsStorageChange(cb: (value: ConsentValue | undefined) => void): () => void;
};

/** Read access to consent for extensions bundled with the tracker. */
export function createConsentHook(getState: () => RuntimeState): ConsentHook {
  return {
    analyticsStorage() {
      const value = getEffectiveConsent(getState).analytics_storage;
      return typeof value === "string" ? value : undefined;
    },
    onAnalyticsStorageChange(cb) {
      const state = getState();
      if (!state.__analyticsStorageListeners) state.__analyticsStorageListeners = [];
      const listeners = state.__analyticsStorageListeners;
      listeners.push(cb);
      return () => {
        const i = listeners.indexOf(cb);
        if (i >= 0) listeners.splice(i, 1);
      };
    },
  };
}
