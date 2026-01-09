import { createRuntimeState } from "./state.ts";
import { maybeEmitConsentUpdatePing } from "./consent_update_ping.ts";
import { isRecord } from "../utils/is_record.ts";
import { ensureArraySlot, getWindowSlot } from "../utils/window_slots.ts";
import type { ConsentState, PropertyConfig, RuntimeState, WindowLike } from "../types.ts";

/**
 * Consumes `window.dataLayer` items pushed by the snippet/global function.
 *
 * - `dataLayer.push(arguments)` where arguments is array-like
 * - the runtime drains existing entries and then patches `push` to intercept
 */
type QueueItem = unknown; // can be `arguments` (array-like) or an actual array
type NormalizedArgs = unknown[] | null;
type DataLayerLike = Array<unknown> & {
  push: (...args: unknown[]) => number;
  __d8aQueueConsumerPatched?: boolean;
  __d8aQueueConsumerOriginalPush?: ((...args: unknown[]) => number) | null;
};

function readUserIdFromObject(v: unknown) {
  if (!isRecord(v)) return null;
  const raw = v.user_id;
  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  return trimmed ? trimmed : null;
}

function readSendPageViewFromConfig(v: unknown) {
  if (!isRecord(v)) return undefined;
  const raw = v.send_page_view;
  return typeof raw === "boolean" ? raw : undefined;
}

function readConsentPatch(v: unknown): ConsentState {
  return isRecord(v) ? v : {};
}

export function createQueueConsumer({
  windowRef,
  dataLayerName = "d8aLayer",
}: {
  windowRef?: WindowLike;
  dataLayerName?: string;
}) {
  if (!windowRef) throw new Error("createQueueConsumer: windowRef is required");
  const w = windowRef;

  const state: RuntimeState = createRuntimeState();
  let started = false;
  let originalPush: ((...args: unknown[]) => number) | null = null;
  let patched = false;

  function normalizeDataLayerItem(item: QueueItem): NormalizedArgs {
    // Snippet pushes `arguments` (array-like, not a real array).
    const maybeArrayLikeLength = isRecord(item) ? item.length : null;
    if (item && typeof maybeArrayLikeLength === "number" && typeof item !== "string") {
      return Array.from(item as ArrayLike<unknown>);
    }
    if (Array.isArray(item)) return item;
    return null;
  }

  function handleCommand(args: unknown[]) {
    const cmd = args[0];
    const a = args[1];
    const b = args[2];
    switch (String(cmd || "")) {
      case "js": {
        // Note: `d8a('js', ...)` is snippet-compatible; callers may pass various values.
        // We preserve runtime behavior by passing the value through to Date() coercion,
        // while keeping TypeScript happy without `any`.
        state.jsDate = a instanceof Date ? a : new Date(a as string | number | Date);
        if (
          state.pageLoadMs == null &&
          state.jsDate instanceof Date &&
          !Number.isNaN(state.jsDate.getTime())
        ) {
          state.pageLoadMs = state.jsDate.getTime();
        }
        return;
      }
      case "config": {
        const propertyId = String(a || "");
        if (!propertyId) return;

        if (!state.propertyIds.includes(propertyId)) state.propertyIds.push(propertyId);

        if (!state.primaryPropertyId) {
          state.primaryPropertyId = propertyId;
        }

        const existingCfg: PropertyConfig = state.propertyConfigs[propertyId] || {};
        const patchCfg: PropertyConfig = isRecord(b) ? b : {};
        state.propertyConfigs[propertyId] = { ...existingCfg, ...patchCfg };

        const userId = readUserIdFromObject(patchCfg);
        if (userId) state.userId = userId;

        const sendPv = readSendPageViewFromConfig(patchCfg);
        if (sendPv !== false && typeof state.__onEvent === "function") {
          // gtag-like: auto page_view on config unless explicitly disabled.
          // Target only the configured property to avoid fan-out duplication per config call.
          state.__onEvent("page_view", { send_to: propertyId });
        }

        if (typeof state.__onConfig === "function") {
          state.__onConfig(propertyId, patchCfg);
        }
        return;
      }
      case "consent": {
        const action = String(a || "");
        if (action === "default" || action === "update") {
          const patch = readConsentPatch(b);
          if (action === "default") {
            state.consentDefault = { ...(state.consentDefault || {}), ...(patch || {}) };
          } else {
            state.consentUpdate = { ...(state.consentUpdate || {}), ...(patch || {}) };
          }
          state.consent = { ...(state.consentDefault || {}), ...(state.consentUpdate || {}) };
          maybeEmitConsentUpdatePing(() => state);
        }
        return;
      }
      case "set": {
        if (isRecord(a)) {
          state.set = { ...(state.set || {}), ...a };
          const userId = readUserIdFromObject(a);
          if (userId) state.userId = userId;
        }
        return;
      }
      case "event": {
        // Store for debugging/tests; the dispatcher is responsible for mapping + sending.
        const params = isRecord(b) ? b : {};
        state.events.push({ name: String(a || ""), params });
        if (typeof state.__onEvent === "function") {
          state.__onEvent(String(a || ""), params);
        }
        return;
      }
      default:
        // Unknown commands are ignored.
        return;
    }
  }

  function drainExisting() {
    const dl = dataLayerName;
    const existing = getWindowSlot<unknown>(w, dl);
    const items = Array.isArray(existing) ? existing : ensureArraySlot<unknown>(w, dl);
    for (const item of items) {
      const args = normalizeDataLayerItem(item);
      if (!args) continue;
      handleCommand(args);
    }
  }

  function patchPush() {
    const dl = dataLayerName;
    const dlArr = ensureArraySlot<unknown>(w, dl) as DataLayerLike;
    if (dlArr.__d8aQueueConsumerPatched) {
      // If already patched, reuse the original push so stop() can restore it.
      originalPush = dlArr.__d8aQueueConsumerOriginalPush || null;
      patched = false;
      return;
    }

    originalPush = dlArr.push.bind(dlArr);
    dlArr.__d8aQueueConsumerOriginalPush = originalPush;
    dlArr.push = function patchedPush(...forwarded: unknown[]) {
      const item = forwarded[0];
      const args = normalizeDataLayerItem(item);
      if (args) handleCommand(args);
      return (originalPush as (...args: unknown[]) => number)(...forwarded);
    };
    dlArr.__d8aQueueConsumerPatched = true;
    patched = true;
  }

  function start() {
    if (started) return;
    started = true;
    drainExisting();
    patchPush();
  }

  return {
    start,
    getState: () => state,
    setOnEvent: (fn: RuntimeState["__onEvent"]) => {
      state.__onEvent = fn;
    },
    setOnConfig: (fn: RuntimeState["__onConfig"]) => {
      state.__onConfig = fn;
    },
    stop: () => {
      if (!started) return;
      const dl = dataLayerName;
      const dlArrMaybe = getWindowSlot<unknown>(w, dl);
      if (patched && originalPush && Array.isArray(dlArrMaybe)) {
        const dlArr = dlArrMaybe as DataLayerLike;
        dlArr.push = originalPush;
        try {
          delete dlArr.__d8aQueueConsumerPatched;
          delete dlArr.__d8aQueueConsumerOriginalPush;
        } catch {
          // ignore
        }
      }
      started = false;
    },
  };
}
