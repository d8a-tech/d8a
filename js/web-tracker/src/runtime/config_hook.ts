import type { RuntimeState } from "../types.ts";

export type ConfigHook = {
  /**
   * Calls `cb` for every property already configured, then for each later `config` call
   * (repeats included). Returns a function that removes the listener.
   */
  onPropertyConfigured(cb: (propertyId: string) => void): () => void;
};

/**
 * Configured properties for extensions bundled with the tracker. The queue consumer runs
 * before an extension can subscribe, so properties configured by then are replayed.
 */
export function createConfigHook(
  getState: () => RuntimeState,
  subscribeToConfig: (cb: (propertyId: string) => void) => void,
): ConfigHook {
  const listeners: Array<(propertyId: string) => void> = [];
  const notify = (cb: (propertyId: string) => void, propertyId: string) => {
    try {
      cb(propertyId);
    } catch {
      // an extension must never break configuration
    }
  };
  subscribeToConfig((propertyId) => {
    for (const cb of listeners.slice()) notify(cb, propertyId);
  });
  return {
    onPropertyConfigured(cb) {
      const configured = getState().propertyIds;
      for (const propertyId of Array.isArray(configured) ? configured.slice() : []) {
        notify(cb, propertyId);
      }
      listeners.push(cb);
      return () => {
        const i = listeners.indexOf(cb);
        if (i >= 0) listeners.splice(i, 1);
      };
    },
  };
}
