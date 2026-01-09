/**
 * User-Agent Client Hints (UA-CH) helpers.
 *
 * - Fetches high-entropy UA-CH values when supported (`navigator.userAgentData`)
 * - Caches the result on `window.d8a_tag_data` so it is fetched at most once per page
 * - Maps cached UA-CH fields into GA4-style query params (uaa/uab/uafvl/...)
 */
const HIGH_ENTROPY_KEYS = [
  "platform",
  "platformVersion",
  "architecture",
  "model",
  "bitness",
  "fullVersionList",
  "wow64",
  "mobile",
] as const;

type UachFullVersion = { brand?: string; version?: string };

type Uach = {
  architecture?: string;
  bitness?: string;
  fullVersionList?: UachFullVersion[];
  mobile?: boolean;
  model?: string;
  platform?: string;
  platformVersion?: string;
  wow64?: boolean;
};

type UserAgentDataLike = {
  // UA-CH API accepts an array of hints; treat it as readonly to allow `as const` lists.
  getHighEntropyValues: (hints: readonly string[]) => Promise<Uach>;
};

type UachStore = {
  uach?: Uach;
  uach_promise?: Promise<Uach>;
};

type WindowLike = import("../types.ts").WindowLike;
type WindowUachCapable = WindowLike & {
  navigator: NonNullable<WindowLike["navigator"]> & { userAgentData: UserAgentDataLike };
};

import { isRecord } from "../utils/is_record.ts";

function getStore(w: WindowLike) {
  // Keep a singleton cache object on window so we only fetch UA-CH values once per page.
  w.d8a_tag_data = w.d8a_tag_data || {};
  const store = w.d8a_tag_data;
  const existing = store.uach_store;
  if (!isRecord(existing)) {
    store.uach_store = {} as UachStore;
  }
  return store.uach_store as UachStore;
}

export function hasUachApi(w: WindowLike): w is WindowUachCapable {
  const nav = w.navigator;
  if (!isRecord(nav)) return false;
  const uad = nav.userAgentData;
  if (!isRecord(uad)) return false;
  const ghev = uad.getHighEntropyValues;
  return typeof ghev === "function";
}

export function getUachCached(w: WindowLike) {
  const store = getStore(w);
  return store.uach || null;
}

export function fetchUach(w: WindowLike) {
  if (!hasUachApi(w)) return null;
  const store = getStore(w);
  if (store.uach_promise) return store.uach_promise;

  store.uach_promise = w.navigator.userAgentData
    .getHighEntropyValues(HIGH_ENTROPY_KEYS)
    .then((d: Uach) => {
      store.uach = store.uach || d;
      return store.uach;
    });
  return store.uach_promise;
}

function enc(v: unknown) {
  return encodeURIComponent(String(v || ""));
}

export function uachToParams(uach: Uach | null) {
  if (!uach) return null;
  const out: Record<string, string> = {};

  if (uach.architecture) out.uaa = String(uach.architecture);
  if (uach.bitness) out.uab = String(uach.bitness);
  if (Array.isArray(uach.fullVersionList)) {
    out.uafvl = uach.fullVersionList
      .map((c) => `${enc(c.brand || "")};${enc(c.version || "")}`)
      .join("|");
  }
  out.uamb = uach.mobile ? "1" : "0";
  if (uach.model) out.uam = String(uach.model);
  if (uach.platform) out.uap = String(uach.platform);
  if (uach.platformVersion) out.uapv = String(uach.platformVersion);
  out.uaw = uach.wow64 ? "1" : "0";

  return out;
}

export const __internal = { HIGH_ENTROPY_KEYS };
