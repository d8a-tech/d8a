import { createD8aGlobal } from "./runtime/d8a_global.ts";
import { createQueueConsumer } from "./runtime/queue_consumer.ts";
import { createDispatcher } from "./runtime/dispatcher.ts";
import {
  resolveDataLayerName,
  resolveGlobalName,
  resolveGtagDataLayerName,
} from "./runtime/runtime_names.ts";
import { createGtagConsentBridge } from "./runtime/gtag_consent_bridge.ts";
import { createEnhancedMeasurement } from "./runtime/enhanced_measurement.ts";
import { createLinker } from "./runtime/linker.ts";
import type { D8aTagData, WindowLike } from "./types.ts";
import { ensureArraySlot, getWindowSlot, setWindowSlot } from "./utils/window_slots.ts";

type InstallResult = {
  consumer: ReturnType<typeof createQueueConsumer>;
  dispatcher: ReturnType<typeof createDispatcher>;
  dataLayerName: string;
  globalName: string;
};

/**
 * Installs the tracker runtime and begins consuming queued `dataLayer` entries.
 *
 * This is the programmatic installer used by both consumption modes:
 * - script-tag bundle (`src/browser_entry.ts`) installs on load
 * - ESM usage (`src/index.ts`) can call this explicitly
 */
export function installD8a({
  windowRef = window,
  dataLayerName,
  globalName,
  gtagDataLayerName,
}: {
  windowRef?: WindowLike;
  dataLayerName?: unknown;
  globalName?: unknown;
  gtagDataLayerName?: unknown;
} = {}) {
  const w = windowRef;
  if (!w) throw new Error("installD8a: window is required");

  // Keep installation records on window, so repeated installs with the
  // same names are safe, but multiple tracker instances can still be created when
  // users provide different `dataLayerName` and/or `globalName` (see example/index.html).
  w.d8a_tag_data = w.d8a_tag_data || {};
  type InstallStores = D8aTagData & {
    __d8aInstallResults?: Record<string, InstallResult>;
    __d8aInstallResultsByDataLayer?: Record<string, InstallResult>;
  };
  const tagData = w.d8a_tag_data as InstallStores;

  const dl = resolveDataLayerName({ windowRef: w, dataLayerName });
  const gn = resolveGlobalName({ windowRef: w, globalName });
  const gdl = resolveGtagDataLayerName({ windowRef: w, gtagDataLayerName });

  if (!tagData.__d8aInstallResults) tagData.__d8aInstallResults = {};
  if (!tagData.__d8aInstallResultsByDataLayer) tagData.__d8aInstallResultsByDataLayer = {};
  const installResults = tagData.__d8aInstallResults;
  const installResultsByDl = tagData.__d8aInstallResultsByDataLayer;
  const key = `${dl}|${gn}`;
  if (installResults[key]) {
    return installResults[key];
  }

  ensureArraySlot<unknown>(w, dl);

  // If the data layer is already being consumed by an existing runtime, create an alias
  // result for this (dl|gn) pair without creating a second consumer/dispatcher.
  const existingForDl = installResultsByDl[dl] || null;
  if (existingForDl) {
    // Ensure the requested global exists and points at this data layer.
    if (typeof getWindowSlot<unknown>(w, gn) !== "function") {
      setWindowSlot(w, gn, createD8aGlobal({ windowRef: w, dataLayerName: dl }));
    }

    const alias = {
      consumer: existingForDl.consumer,
      dispatcher: existingForDl.dispatcher,
      dataLayerName: dl,
      globalName: gn,
    };
    installResults[key] = alias;
    return alias;
  }

  // If the user already defined a snippet `d8a()` (or custom global) that pushes into dataLayer,
  // leave it alone. Otherwise create the global function.
  if (typeof getWindowSlot<unknown>(w, gn) !== "function") {
    setWindowSlot(w, gn, createD8aGlobal({ windowRef: w, dataLayerName: dl }));
  }

  const consumer = createQueueConsumer({ windowRef: w, dataLayerName: dl });

  const onConfigCbs: Array<(propertyId: string, patchCfg: Record<string, unknown>) => void> = [];
  consumer.setOnConfig((propertyId, patchCfg) => {
    for (const cb of onConfigCbs) cb(propertyId, patchCfg);
  });

  const onSetCbs: Array<(args: unknown) => void> = [];
  consumer.setOnSet((args) => {
    for (const cb of onSetCbs) cb(args);
  });

  const dispatcher = createDispatcher({ windowRef: w, getState: consumer.getState });
  dispatcher.attachLifecycleFlush();
  consumer.setOnEvent((name: string, params: Record<string, unknown>) => {
    dispatcher.enqueueEvent(name, params);
  });

  // Cross-domain linker (`_dl`) for client/session continuity across unrelated domains.
  const linker = createLinker({ windowRef: w, getState: consumer.getState });
  linker.start();
  // Apply pending incoming payload when:
  // - linker config changes (accept_incoming/domains)
  // - new properties are configured (cookie settings become known)
  onSetCbs.push((args) => {
    // Keep shared linker config in sync (when multiple runtimes are installed).
    if (
      args &&
      typeof args === "object" &&
      (args as any).type === "field" &&
      (args as any).field === "linker"
    ) {
      linker.updateConfigPatch((args as any).value);
    }
    if (args && typeof args === "object" && (args as any).type === "object") {
      linker.updateConfigPatch((args as any)?.obj?.linker);
    }
    linker.applyIncomingIfReady();
  });
  onConfigCbs.push(() => linker.applyIncomingIfReady());

  // If gtag/GTM is present, prefer consent pushed into `window.dataLayer`.
  // This runs regardless of d8a's own dataLayerName to avoid requiring users
  // to align queue names.
  const gtagConsentBridge = createGtagConsentBridge({
    windowRef: w,
    getState: consumer.getState,
    dataLayerName: gdl,
  });

  consumer.start();
  gtagConsentBridge.start();

  // Enhanced measurement
  try {
    const em = createEnhancedMeasurement({ windowRef: w, getState: consumer.getState, dispatcher });
    onConfigCbs.push(() => em.onConfig());
    em.start();
  } catch {
    // ignore
  }

  const result = { consumer, dispatcher, dataLayerName: dl, globalName: gn };
  installResults[key] = result;
  installResultsByDl[dl] = result;
  return result;
}
