// Test-only utilities and mock types.
// This file intentionally has no runtime side effects.

import type { WindowLike } from "../src/types.ts";
import { ensureArraySlot, getWindowSlot } from "../src/utils/window_slots.ts";

// Re-export for convenience in tests
export { ensureArraySlot, getWindowSlot };

export const PROPERTY_ID = "80e1d6d0-560d-419f-ac2a-fe9281e93386";
export const TRACKER_DOMAIN = "https://tracker.example.test";

export function tick() {
  return new Promise((r) => setImmediate(r));
}

export function getD8a(w: WindowLike, name = "d8a") {
  const fn = getWindowSlot(w, name);
  if (typeof fn !== "function") throw new Error(`${name} not installed`);
  return fn as (...args: unknown[]) => void;
}

export function installSnippetShim(w: WindowLike, globalName: string, dataLayerName: string) {
  if (typeof getWindowSlot(w, globalName) === "function") return;

  // Simulate the snippet logic: create a function that pushes `arguments` to the data layer array.
  const queue = ensureArraySlot<unknown>(w, dataLayerName);
  (w as Record<string, unknown>)[globalName] = function () {
    queue.push(arguments as unknown);
  };
}

export type Listener = (...args: unknown[]) => void;
export type ListenerMap = Map<string, Listener[]>;
export type FetchCall = { url: string; opts?: unknown };

export type WindowMock = WindowLike & {
  // Introspection helpers for tests.
  __listeners: ListenerMap;
  __docListeners: ListenerMap;
  fetchCalls: FetchCall[];
  // Provide fetch so dispatcher can use transport/fetch path.
  fetch: (url: string, opts?: unknown) => Promise<{ status: number }>;
} & Record<string, unknown>;

function pushListener(map: ListenerMap, type: string, fn: Listener) {
  const arr = map.get(type);
  if (arr) arr.push(fn);
  else map.set(type, [fn]);
}

export function makeWindowMock(
  overrides: Partial<Omit<WindowMock, "document">> & {
    document?: Partial<WindowMock["document"]>;
  } = {},
): WindowMock {
  const listeners: ListenerMap = new Map();
  const docListeners: ListenerMap = new Map();

  const baseLocation: WindowMock["location"] = {
    href: "https://docs.example.test/",
    hostname: "docs.example.test",
    protocol: "https:",
  };
  const baseNavigator: WindowMock["navigator"] = { language: "en-gb", sendBeacon: null };
  const baseScreen: WindowMock["screen"] = { width: 5120, height: 1440 };
  const baseDocument: WindowMock["document"] = {
    title: "My Dev",
    hidden: false,
    cookie: "",
    addEventListener: (t: string, fn: Listener) => pushListener(docListeners, t, fn),
  };

  // Note: override objects are shallow-merged so callers can change fields without
  // accidentally dropping required functions like document.addEventListener.
  const mergedLocation = { ...baseLocation, ...(overrides.location || {}) };
  const mergedNavigator = { ...baseNavigator, ...(overrides.navigator || {}) };
  const mergedScreen = { ...baseScreen, ...(overrides.screen || {}) };
  const mergedDocument = { ...baseDocument, ...(overrides.document || {}) };

  const rest: any = { ...overrides };
  delete rest.location;
  delete rest.navigator;
  delete rest.screen;
  delete rest.document;

  const w: WindowMock = {
    location: mergedLocation,
    navigator: mergedNavigator,
    screen: mergedScreen,
    document: mergedDocument,
    addEventListener: (t: string, fn: Listener) => pushListener(listeners, t, fn),
    setTimeout: (fn: Listener) => {
      fn();
      return 1;
    },
    clearTimeout: () => {},
    fetchCalls: [],
    fetch: async (url: string, opts?: unknown) => {
      w.fetchCalls.push({ url, opts });
      return { status: 0 };
    },
    __listeners: listeners,
    __docListeners: docListeners,
    ...(rest as Omit<Partial<WindowMock>, "location" | "navigator" | "screen" | "document">),
  };

  return w;
}
