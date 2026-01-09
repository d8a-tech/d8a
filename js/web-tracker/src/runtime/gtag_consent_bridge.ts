/**
 * Mirrors Consent Mode commands pushed into `window.dataLayer` (via `gtag(...)`)
 * into d8a runtime state.
 *
 * Requirement: prefer gtag consent state if available; fall back to d8a consent
 * otherwise.
 *
 * We intentionally only consume `consent` commands and leave all other gtag/GTM
 * plumbing alone.
 */
import { maybeEmitConsentUpdatePing } from "./consent_update_ping.ts";
import { isRecord } from "../utils/is_record.ts";
import type { ConsentState, RuntimeState, WindowLike } from "../types.ts";
import { ensureArraySlot, getWindowSlot, setWindowSlot } from "../utils/window_slots.ts";

const HUBS_KEY = "__d8aConsentBridgeHubs";

type DataLayerLike = Array<unknown> & {
  push: (...args: unknown[]) => number;
  __d8aConsentBridgePatched?: boolean;
};

type DataLayerHub = {
  subscribers: Set<(args: unknown[]) => void>;
  originalPush: ((...args: unknown[]) => number) | null;
  patched: boolean;
  dataLayerName: string;
};

type HubsMap = Record<string, DataLayerHub>;

type ConsentAction = "default" | "update";
type GtagConsentCommand = ["consent", ConsentAction, ConsentState];

function isHubsMap(v: unknown): v is HubsMap {
  // This slot is owned by this module, so the same `isRecord()` check we used before
  // is sufficient for our type purposes (behavior-preserving).
  return isRecord(v);
}

function getHub(w: WindowLike, dataLayerName: unknown) {
  if (!w) return null;
  const dl = String(dataLayerName || "dataLayer");
  const existingHubs = getWindowSlot<unknown>(w, HUBS_KEY);
  const hubs: HubsMap = isHubsMap(existingHubs) ? existingHubs : {};
  if (!existingHubs || existingHubs !== hubs) setWindowSlot(w, HUBS_KEY, hubs);

  const existing = hubs[dl];
  if (existing && typeof existing === "object") return existing;

  const hub: DataLayerHub = {
    subscribers: new Set<(args: unknown[]) => void>(),
    originalPush: null,
    patched: false,
    dataLayerName: dl,
  };
  hubs[dl] = hub;
  return hub;
}

export function createGtagConsentBridge({
  windowRef,
  getState,
  dataLayerName = "dataLayer",
}: {
  windowRef?: WindowLike;
  getState: () => RuntimeState;
  dataLayerName?: string;
}) {
  if (!windowRef) throw new Error("createGtagConsentBridge: windowRef is required");
  const w = windowRef;
  if (!w) throw new Error("createGtagConsentBridge: windowRef is required");
  if (typeof getState !== "function")
    throw new Error("createGtagConsentBridge: getState is required");

  let started = false;
  let unsubscribe: null | (() => void) = null;

  function normalizeDataLayerItem(item: unknown) {
    // gtag snippet pushes `arguments` (array-like), but sometimes arrays are used.
    const maybeArrayLikeLength = isRecord(item) ? item.length : null;
    if (item && typeof maybeArrayLikeLength === "number" && typeof item !== "string") {
      try {
        return Array.from(item as ArrayLike<unknown>);
      } catch {
        return null;
      }
    }
    if (Array.isArray(item)) return item;
    return null;
  }

  function applyConsentPatch({ action, patch }: { action: string; patch: ConsentState }) {
    const state = getState();
    if (!state) return;

    if (action === "default") {
      state.consentDefaultGtag = { ...(state.consentDefaultGtag || {}), ...(patch || {}) };
    } else if (action === "update") {
      state.consentUpdateGtag = { ...(state.consentUpdateGtag || {}), ...(patch || {}) };
    } else {
      return;
    }
    state.consentGtag = { ...(state.consentDefaultGtag || {}), ...(state.consentUpdateGtag || {}) };
    maybeEmitConsentUpdatePing(getState);
  }

  function parseConsentCommand(args: unknown[]): GtagConsentCommand | null {
    const [cmd, a, b] = args;
    if (cmd !== "consent") return null;
    const action = String(a || "");
    if (action !== "default" && action !== "update") return null;
    if (!isRecord(b)) return null;
    return ["consent", action, b];
  }

  function handleArgs(args: unknown[]) {
    const c = parseConsentCommand(args);
    if (!c) return;
    const [, action, patch] = c;
    applyConsentPatch({ action, patch });
  }

  function drainExisting() {
    const dl = dataLayerName;
    const dlArr = getWindowSlot<unknown>(w, dl);
    if (!Array.isArray(dlArr)) return;
    for (const item of dlArr) {
      const args = normalizeDataLayerItem(item);
      if (!args) continue;
      handleArgs(args);
    }
  }

  function patchPush() {
    const dl = dataLayerName;
    const dlArr = ensureArraySlot<unknown>(w, dl) as DataLayerLike;

    const hub = getHub(w, dl);
    if (!hub) return;

    // Avoid double-patching (hub fan-outs to all subscribers).
    if (hub.patched) return;

    hub.originalPush = dlArr.push.bind(dlArr);
    dlArr.push = function patchedPush(...forwarded: unknown[]) {
      const item = forwarded[0];
      const args = normalizeDataLayerItem(item);
      if (args) {
        for (const sub of hub.subscribers) {
          try {
            sub(args);
          } catch {
            // ignore subscriber errors
          }
        }
      }
      return (hub.originalPush as (...args: unknown[]) => number)(...forwarded);
    };
    hub.patched = true;
    dlArr.__d8aConsentBridgePatched = true;
  }

  function start() {
    if (started) return;
    started = true;

    const hub = getHub(w, dataLayerName);
    if (!hub) return;
    hub.subscribers.add(handleArgs);
    unsubscribe = () => {
      hub.subscribers.delete(handleArgs);
    };

    drainExisting();
    patchPush();
  }

  return {
    start,
    stop: () => {
      if (!started) return;
      const dl = dataLayerName;
      const hub = getHub(w, dl);
      if (!hub) return;
      if (typeof unsubscribe === "function") unsubscribe();
      unsubscribe = null;

      const dlArrMaybe = getWindowSlot<unknown>(w, dl);
      if (hub.subscribers.size === 0 && hub.originalPush && Array.isArray(dlArrMaybe)) {
        const dlArr = dlArrMaybe as DataLayerLike;
        dlArr.push = hub.originalPush;
        hub.originalPush = null;
        hub.patched = false;
        try {
          delete dlArr.__d8aConsentBridgePatched;
          const maybeHubs = getWindowSlot<unknown>(w, HUBS_KEY);
          const hubs = isRecord(maybeHubs) ? (maybeHubs as HubsMap) : null;
          if (hubs) {
            delete hubs[dl];
            if (Object.keys(hubs).length === 0) setWindowSlot(w, HUBS_KEY, undefined);
          }
        } catch {
          // ignore
        }
      }
      started = false;
    },
  };
}
