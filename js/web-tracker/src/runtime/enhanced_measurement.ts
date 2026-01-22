import { getPropertyConfig, getPropertyIds, getSetParams } from "./config_resolver.ts";
import type { RuntimeState, WindowLike } from "../types.ts";
import { isRecord } from "../utils/is_record.ts";
import { ensureArraySlot } from "../utils/window_slots.ts";

function parseList(v: unknown, fallback: string[] = []) {
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean);
  if (typeof v === "string") {
    return v
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return fallback;
}

function safeUrl(href: unknown, baseHref: unknown) {
  try {
    return new URL(String(href || ""), baseHref ? String(baseHref) : undefined);
  } catch {
    return null;
  }
}

type NodeLike = Record<string, unknown> & { parentNode?: unknown; tagName?: unknown };
type ElementLike = NodeLike & { getAttribute?: (name: string) => unknown };

function closestAnchor(node: unknown) {
  let n = node;
  for (let i = 0; i < 100 && n; i += 1) {
    const nn = isRecord(n) ? (n as NodeLike) : null;
    const tag = String(nn?.tagName || "").toLowerCase();
    if (tag === "a" || tag === "area") return n;
    n = (nn?.parentNode as unknown) || null;
  }
  return null;
}

function getAttr(el: unknown, name: string) {
  if (!el) return null;
  const ee = isRecord(el) ? (el as ElementLike) : null;
  try {
    if (typeof ee?.getAttribute === "function") {
      const attr = ee.getAttribute(name);
      if (attr != null) return attr;
    }
  } catch {
    // ignore
  }
  const v = ee ? ee[name] : null;
  return v != null ? String(v) : null;
}

function isHttpUrl(u: unknown) {
  const p = String((u as { protocol?: unknown } | null)?.protocol || "").toLowerCase();
  return p === "http:" || p === "https:";
}

function hostnameMatches(host: unknown, domain: unknown) {
  const h = String(host || "").toLowerCase();
  const d = String(domain || "")
    .toLowerCase()
    .replace(/^\./, "");
  if (!h || !d) return false;
  return h === d || h.endsWith(`.${d}`);
}

function fileNameFromUrl(u: unknown) {
  const p = String((u as { pathname?: unknown } | null)?.pathname || "");
  const seg = p.split("/").filter(Boolean).slice(-1)[0] || "";
  return seg;
}

function fileExtension(name: unknown) {
  const n = String(name || "");
  const idx = n.lastIndexOf(".");
  if (idx < 0) return "";
  return n.slice(idx + 1).toLowerCase();
}

function normalizeWhitespace(s: unknown) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim();
}

function getLinkId(a: unknown) {
  const v = getAttr(a, "id");
  const out = normalizeWhitespace(v);
  return out || null;
}

function getLinkClasses(a: unknown) {
  // Most reliable is className; fall back to attribute.
  const v = getAttr(a, "className") || getAttr(a, "class");
  const out = normalizeWhitespace(v);
  return out || null;
}

function getLinkText(a: unknown) {
  const v = getAttr(a, "textContent") || getAttr(a, "innerText");
  const out = normalizeWhitespace(v);
  return out || null;
}

export function createEnhancedMeasurement({
  windowRef,
  getState,
  dataLayerName,
  dispatcher,
}: {
  windowRef: WindowLike;
  getState: () => RuntimeState;
  dataLayerName: string;
  dispatcher: {
    enqueueEvent: (name: string, params: Record<string, unknown>) => void;
    flushNow?: (opts?: { useBeacon?: boolean }) => unknown;
  };
}) {
  const w = windowRef;
  if (!w) throw new Error("createEnhancedMeasurement: windowRef is required");
  if (typeof getState !== "function")
    throw new Error("createEnhancedMeasurement: getState is required");
  if (!dataLayerName) throw new Error("createEnhancedMeasurement: dataLayerName is required");
  if (!dispatcher) throw new Error("createEnhancedMeasurement: dispatcher is required");

  type BoolKey = "site_search_enabled" | "outbound_clicks_enabled" | "file_downloads_enabled";
  type ListKey =
    | "site_search_query_params"
    | "outbound_exclude_domains"
    | "file_download_extensions";

  function readBoolKey(source: Record<string, unknown>, key: BoolKey): boolean | undefined {
    const v = source[key];
    return typeof v === "boolean" ? v : undefined;
  }

  function readListKey(source: Record<string, unknown>, key: ListKey): unknown {
    return source[key];
  }

  function isEnabledForProperty({
    propertyId,
    key,
    defaultValue,
  }: {
    propertyId: string;
    key: BoolKey;
    defaultValue: boolean;
  }) {
    const setParams = getSetParams(getState);
    const cfg = getPropertyConfig(getState, propertyId);
    const c = readBoolKey(cfg, key);
    if (c != null) return c;
    const s = readBoolKey(setParams, key);
    if (s != null) return s;
    return defaultValue;
  }

  function getListForProperty({
    propertyId,
    key,
    defaultList,
  }: {
    propertyId: string;
    key: ListKey;
    defaultList: string[];
  }) {
    const setParams = getSetParams(getState);
    const cfg = getPropertyConfig(getState, propertyId);
    const cfgV = readListKey(cfg, key);
    const setV = readListKey(setParams, key);
    if (cfgV != null) return parseList(cfgV, defaultList);
    if (setV != null) return parseList(setV, defaultList);
    return defaultList;
  }

  function markInstalledOnce() {
    const s = getState();
    if (!s) return false;
    if (s.emInstalled) return false;
    s.emInstalled = true;
    return true;
  }

  function markSiteSearchOnce() {
    const s = getState();
    if (!s) return false;
    if (s.emSiteSearchFired) return false;
    s.emSiteSearchFired = true;
    return true;
  }

  function maybeFireSiteSearch() {
    const propertyIds = getPropertyIds(getState);
    // If no properties are configured yet, do nothing (and don't mark fired).
    if (propertyIds.length === 0) return;

    // Only run once per page load per instance (user request: no SPA tracking).
    if (!markSiteSearchOnce()) return;

    const search = String(w?.location?.search || "");
    if (!search || search === "?") return;
    const sp = new URLSearchParams(search.startsWith("?") ? search.slice(1) : search);

    const defaultKeys = ["q", "s", "search", "query", "keyword"];

    // Determine which properties have a search term, and group by term value.
    const termToProps = new Map<string, string[]>();
    for (const pid of propertyIds) {
      const enabled = isEnabledForProperty({
        propertyId: pid,
        key: "site_search_enabled",
        defaultValue: true,
      });
      if (!enabled) continue;
      const keys = getListForProperty({
        propertyId: pid,
        key: "site_search_query_params",
        defaultList: defaultKeys,
      });
      let found = "";
      for (const k of keys) {
        const v = sp.get(k);
        if (v && String(v).trim()) {
          found = String(v).trim();
          break;
        }
      }
      if (!found) continue;
      const list = termToProps.get(found) || [];
      list.push(pid);
      termToProps.set(found, list);
    }

    for (const [term, pids] of termToProps.entries()) {
      // Push to dataLayer so GTM can filter it
      const dataLayer = ensureArraySlot(w, dataLayerName);
      dataLayer.push(["event", "view_search_results", { search_term: term, send_to: pids }]);
    }
  }

  function shouldTrackOutbound({
    propertyId,
    linkHostname,
  }: {
    propertyId: string;
    linkHostname: string;
  }) {
    const enabled = isEnabledForProperty({
      propertyId,
      key: "outbound_clicks_enabled",
      defaultValue: true,
    });
    if (!enabled) return false;

    const excludes = getListForProperty({
      propertyId,
      key: "outbound_exclude_domains",
      defaultList: [String(w?.location?.hostname || "")],
    });
    for (const d of excludes) {
      if (hostnameMatches(linkHostname, d)) return false;
    }
    return true;
  }

  function shouldTrackDownload({ propertyId, ext }: { propertyId: string; ext: string }) {
    const enabled = isEnabledForProperty({
      propertyId,
      key: "file_downloads_enabled",
      defaultValue: true,
    });
    if (!enabled) return false;
    const list = getListForProperty({
      propertyId,
      key: "file_download_extensions",
      defaultList: [
        "pdf",
        "doc",
        "docx",
        "xls",
        "xlsx",
        "ppt",
        "pptx",
        "csv",
        "txt",
        "rtf",
        "zip",
        "rar",
        "7z",
        "dmg",
        "exe",
        "apk",
      ],
    });
    const set = new Set(list.map((x) => String(x).toLowerCase().replace(/^\./, "")));
    return !!ext && set.has(String(ext).toLowerCase().replace(/^\./, ""));
  }

  function onClick(evt: unknown) {
    const propertyIds = getPropertyIds(getState);
    if (propertyIds.length === 0) return;

    const target = isRecord(evt) ? evt.target : null;
    const a = closestAnchor(target);
    if (!a) return;

    const href = getAttr(a, "href");
    if (!href) return;
    const u = safeUrl(href, String(w?.location?.href || ""));
    if (!u || !isHttpUrl(u)) return;

    const linkUrl = String(u.href || "");
    const linkHostname = String(u.hostname || "");

    const linkId = getLinkId(a);
    const linkClasses = getLinkClasses(a);
    const linkText = getLinkText(a);

    const fileName = fileNameFromUrl(u) || getAttr(a, "download") || "";
    const ext = fileExtension(fileName);

    // Determine destinations per-event (per-property config).
    const downloadDestinations: string[] = [];
    const outboundDestinations: string[] = [];
    for (const pid of propertyIds) {
      if (shouldTrackDownload({ propertyId: pid, ext })) downloadDestinations.push(pid);
      if (shouldTrackOutbound({ propertyId: pid, linkHostname })) outboundDestinations.push(pid);
    }

    // Prefer file_download over outbound click if both apply.
    if (downloadDestinations.length > 0) {
      // Push to dataLayer so GTM can filter it
      const dataLayer = ensureArraySlot(w, dataLayerName);
      dataLayer.push([
        "event",
        "file_download",
        {
          link_url: linkUrl,
          ...(linkId ? { link_id: linkId } : {}),
          ...(linkClasses ? { link_classes: linkClasses } : {}),
          ...(linkText ? { link_text: linkText } : {}),
          file_name: fileName,
          file_extension: ext,
          send_to: downloadDestinations,
        },
      ]);
      // For click-driven events prefer fetch(keepalive) instead of sendBeacon,
      // since some browsers/extensions block beacons/pings by default.
      dispatcher.flushNow?.({ useBeacon: false });
      return;
    }

    if (outboundDestinations.length > 0) {
      // Push to dataLayer so GTM can filter it
      const dataLayer = ensureArraySlot(w, dataLayerName);
      dataLayer.push([
        "event",
        "click",
        {
          link_url: linkUrl,
          ...(linkId ? { link_id: linkId } : {}),
          ...(linkClasses ? { link_classes: linkClasses } : {}),
          link_domain: linkHostname,
          outbound: "1",
          send_to: outboundDestinations,
        },
      ]);
      // For click-driven events prefer fetch(keepalive) instead of sendBeacon,
      // since some browsers/extensions block beacons/pings by default.
      dispatcher.flushNow?.({ useBeacon: false });
    }
  }

  function start() {
    // Attach listeners only once per instance.
    if (!markInstalledOnce()) return;

    // Site search: check once on page load (after config is available). If config
    // isn't available yet (programmatic installs), retry once asynchronously.
    try {
      maybeFireSiteSearch();
      if (!getState()?.emSiteSearchFired && typeof w?.setTimeout === "function") {
        w.setTimeout(() => {
          try {
            maybeFireSiteSearch();
          } catch {
            // ignore
          }
        }, 0);
      }
    } catch {
      // ignore
    }

    // Outbound/download: click listeners.
    const doc = w?.document;
    if (doc && typeof doc.addEventListener === "function") {
      doc.addEventListener("click", onClick, true);
      doc.addEventListener("auxclick", onClick, true);
    }
  }

  function onConfig() {
    try {
      maybeFireSiteSearch();
    } catch {
      // ignore
    }
  }

  return { start, onConfig };
}
