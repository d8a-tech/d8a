function safeString(v: unknown) {
  return typeof v === "string" ? v : "";
}

import { isRecord } from "../utils/is_record.ts";

type CurrentScriptLike = { src?: unknown };
type DocumentLike = { currentScript?: unknown; location?: { href?: unknown } };

function detectFromCurrentScript(documentRef: unknown, key: string) {
  try {
    const doc = isRecord(documentRef) ? (documentRef as DocumentLike) : null;
    const cs = doc && isRecord(doc.currentScript) ? (doc.currentScript as CurrentScriptLike) : null;
    const src = safeString(cs?.src);
    if (!src) return "";
    const baseHref = doc && isRecord(doc.location) ? safeString(doc.location?.href) : "";
    const u = new URL(src, baseHref || undefined);
    return safeString(u.searchParams.get(key));
  } catch {
    return "";
  }
}

export function resolveDataLayerName({
  windowRef,
  dataLayerName,
}: {
  windowRef?: import("../types.ts").WindowLike;
  dataLayerName?: unknown;
}) {
  // Explicit override (ESM usage or programmatic install)
  if (typeof dataLayerName === "string" && dataLayerName.trim()) return dataLayerName.trim();

  // Optional global hint (script-tag usage if desired)
  const hinted = safeString(windowRef?.d8aDataLayerName);
  if (hinted.trim()) return hinted.trim();

  // Script src query param `?l=...` (gtag-compatible)
  const fromScript = detectFromCurrentScript(windowRef?.document, "l");
  if (fromScript.trim()) return fromScript.trim();

  // Default to avoid collisions with GTM/gtag which commonly use `dataLayer`.
  return "d8aLayer";
}

export function resolveGlobalName({
  windowRef,
  globalName,
}: {
  windowRef?: import("../types.ts").WindowLike;
  globalName?: unknown;
}) {
  // Explicit override (ESM usage or programmatic install)
  if (typeof globalName === "string" && globalName.trim()) return globalName.trim();

  // Optional global hint (script-tag usage if desired)
  const hinted = safeString(windowRef?.d8aGlobalName);
  if (hinted.trim()) return hinted.trim();

  // Script src query param `?g=...` (gtag-like convention)
  const fromScript = detectFromCurrentScript(windowRef?.document, "g");
  if (fromScript.trim()) return fromScript.trim();

  return "d8a";
}

export function resolveGtagDataLayerName({
  windowRef,
  gtagDataLayerName,
}: {
  windowRef?: import("../types.ts").WindowLike;
  gtagDataLayerName?: unknown;
}) {
  // Explicit override (ESM usage or programmatic install)
  if (typeof gtagDataLayerName === "string" && gtagDataLayerName.trim())
    return gtagDataLayerName.trim();

  // Optional global hint (script-tag usage if desired)
  const hinted = safeString(windowRef?.d8aGtagDataLayerName);
  if (hinted.trim()) return hinted.trim();

  // Script src query param `?gl=...` (gtag consent queue name)
  const fromScript = detectFromCurrentScript(windowRef?.document, "gl");
  if (fromScript.trim()) return fromScript.trim();

  // Default gtag/GTM queue name.
  return "dataLayer";
}
