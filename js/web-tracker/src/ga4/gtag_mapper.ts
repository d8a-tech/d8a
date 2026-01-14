import {
  buildD8aClientCookieName,
  buildD8aCookieName,
  parseD8aClientCookie,
  parseD8aSessionCookie,
} from "../cookies/d8a_cookies.ts";
import { parseCookieHeader } from "../cookies/cookie_jar.ts";
import type { BrowserContext, ConsentState } from "../types.ts";

const CORE_ECOMMERCE_NUMBER = new Set(["value", "tax", "shipping"]);

const ITEM_KEY_MAP: Record<string, string> = {
  item_id: "id",
  item_name: "nm",
  affiliation: "af",
  coupon: "cp",
  discount: "ds",
  index: "lp",
  item_brand: "br",
  item_category: "ca",
  item_category2: "c2",
  item_category3: "c3",
  item_category4: "c4",
  item_category5: "c5",
  item_list_id: "li",
  item_list_name: "ln",
  item_variant: "va",
  location_id: "lo",
  price: "pr",
  quantity: "qt",
  promotion_id: "pi",
  promotion_name: "pn",
};

function isScalar(v: unknown) {
  return v == null || typeof v === "string" || typeof v === "number" || typeof v === "boolean";
}

// Note: we intentionally treat arrays as "object-like" here to preserve the historical
// runtime behavior of `encodeItemToPrValue` (previously `typeof v === "object"`).
function isObjectLike(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === "object";
}

function toEpValue(v: unknown) {
  if (v == null) return null;
  if (typeof v === "string") return v;
  if (typeof v === "boolean") return v ? "1" : "0";
  if (typeof v === "number") return String(v);
  return String(v);
}

function toEpnValue(v: unknown) {
  if (typeof v === "number") return v;
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function addEp(params: URLSearchParams, key: string, value: unknown) {
  if (value == null) return;
  params.set(`ep.${key}`, String(value));
}

function addEpn(params: URLSearchParams, key: string, value: unknown) {
  if (value == null) return;
  // Keep numeric formatting stable (72.05 not "72.0500")
  params.set(`epn.${key}`, String(value));
}

export function encodeItemToPrValue(item: unknown) {
  const it = isObjectLike(item) ? item : {};

  const knownParts: string[] = [];
  const customParts: string[] = [];

  // Preserve a stable, GA-like ordering: known mapped keys first (in map order),
  // then custom key/value tokens.
  for (const [srcKey, dstKey] of Object.entries(ITEM_KEY_MAP)) {
    if (!(srcKey in it)) continue;
    const v = it[srcKey];
    if (!isScalar(v)) continue;
    const str = toEpValue(v);
    if (str == null) continue;
    knownParts.push(`${dstKey}${str}`);
  }

  // Custom params: any scalar key not in ITEM_KEY_MAP.
  let idx = 0;
  for (const key of Object.keys(it)) {
    if (key in ITEM_KEY_MAP) continue;
    const v = it[key];
    if (!isScalar(v)) continue;
    const str = toEpValue(v);
    if (str == null) continue;
    customParts.push(`k${idx}${key}`);
    customParts.push(`v${idx}${str}`);
    idx += 1;
  }

  return [...knownParts, ...customParts].join("~");
}

/**
 * Builds the query params for a GA4 gtag `/g/collect` request.
 *
 * This returns a URLSearchParams with already-stringified values. The caller is
 * responsible for attaching it to a URL or sending as body.
 */
export function buildGa4CollectQueryParams({
  propertyId,
  eventName,
  eventParams,
  cookieHeader,
  clientId,
  userId,
  cookiePrefix,
  ignoreReferrer,
  browser,
  pageLoadMs,
  hitCounter,
  engagementTimeMs,
  uachParams,
  campaign,
  contentGroup,
  consentParams,
  debugMode,
}: {
  propertyId: string;
  eventName: string;
  eventParams: Record<string, unknown>;
  cookieHeader: string;
  clientId: string | null;
  userId: string | null;
  cookiePrefix: string;
  ignoreReferrer: boolean;
  browser: BrowserContext;
  pageLoadMs: number | null;
  hitCounter: number | null;
  engagementTimeMs: number | null;
  uachParams: Record<string, string> | null;
  campaign: {
    campaign_id?: string | null;
    campaign_source?: string | null;
    campaign_medium?: string | null;
    campaign_name?: string | null;
    campaign_term?: string | null;
    campaign_content?: string | null;
  } | null;
  contentGroup: string | null;
  consentParams: { gcs?: string | null; gcd?: string | null; consent?: ConsentState } | null;
  debugMode: boolean;
}) {
  const params = new URLSearchParams();

  // Core request identity.
  params.set("v", "2");
  params.set("tid", String(propertyId || ""));
  params.set("en", String(eventName || ""));

  // GA4-style debug mode:
  // - `_dbg=1` enables DebugView-style ingestion on the backend
  // - `ep.debug_mode=1` is included for parity with gtag semantics
  if (debugMode === true) params.set("_dbg", "1");

  if (pageLoadMs != null) params.set("_p", String(pageLoadMs));
  if (hitCounter != null) params.set("_s", String(hitCounter));
  if (engagementTimeMs != null) params.set("_et", String(engagementTimeMs));

  // Cookies.
  const cookieMap = parseCookieHeader(cookieHeader);

  // Client id:
  // - If `clientId` is provided, it wins (used for cookieless / consent denied).
  // - Otherwise derive from the first-party `_d8a` cookie.
  if (clientId != null && String(clientId)) {
    params.set("cid", String(clientId));
  } else {
    const client = parseD8aClientCookie(cookieMap.get(buildD8aClientCookieName(cookiePrefix)));
    if (client?.cid) params.set("cid", client.cid);
  }

  // User id (provided by caller): maps to GA `uid`
  if (typeof userId === "string" && userId.trim()) {
    params.set("uid", userId.trim());
  }

  // Campaign overrides. These map to standard GA parameters.
  const c = campaign || {};
  if (typeof c.campaign_id === "string" && c.campaign_id.trim())
    params.set("ci", c.campaign_id.trim());
  if (typeof c.campaign_source === "string" && c.campaign_source.trim())
    params.set("cs", c.campaign_source.trim());
  if (typeof c.campaign_medium === "string" && c.campaign_medium.trim())
    params.set("cm", c.campaign_medium.trim());
  if (typeof c.campaign_name === "string" && c.campaign_name.trim())
    params.set("cn", c.campaign_name.trim());
  if (typeof c.campaign_term === "string" && c.campaign_term.trim())
    params.set("ct", c.campaign_term.trim());
  if (typeof c.campaign_content === "string" && c.campaign_content.trim())
    params.set("cc", c.campaign_content.trim());

  if (typeof contentGroup === "string" && contentGroup.trim()) {
    // Use event-param namespace for content group as requested.
    addEp(params, "content_group", contentGroup.trim());
  }

  const sessionCookieName = buildD8aCookieName(propertyId, cookiePrefix);
  const session = parseD8aSessionCookie(cookieMap.get(sessionCookieName));

  // sid: use cookie value or default to current timestamp (seconds since epoch)
  if (session?.sid != null) {
    params.set("sid", String(session.sid));
  } else {
    params.set("sid", String(Math.floor(Date.now() / 1000)));
  }

  // sct: use cookie value or default to 1
  if (session?.sct != null) {
    params.set("sct", String(session.sct));
  } else {
    params.set("sct", "1");
  }

  if (session?.seg != null) params.set("seg", String(session.seg));

  const b = browser;

  // Browser-derived.
  if (b.dl) params.set("dl", String(b.dl));
  if (b.dt) params.set("dt", String(b.dt));
  // ignore_referrer: include ir=1 only when explicitly enabled
  if (ignoreReferrer === true) params.set("ir", "1");
  // Send empty referrer explicitly (GA commonly sends `dr=` for direct).
  params.set("dr", String(b.dr));
  if (b.ul) params.set("ul", String(b.ul));
  if (b.sr) params.set("sr", String(b.sr));

  if (uachParams) {
    for (const [k, v] of Object.entries(uachParams)) {
      if (v == null || v === "") continue;
      params.set(k, String(v));
    }
  }

  if (consentParams) {
    if (consentParams.gcs) params.set("gcs", String(consentParams.gcs));
    if (consentParams.gcd) params.set("gcd", String(consentParams.gcd));
  }

  const p = eventParams || {};

  if (debugMode === true && !Object.prototype.hasOwnProperty.call(p, "debug_mode")) {
    addEp(params, "debug_mode", "1");
  }

  // Ecommerce / reserved keys.
  const currency = p["currency"];
  if (typeof currency === "string") params.set("cu", currency);
  if (p["transaction_id"] != null) addEp(params, "transaction_id", toEpValue(p["transaction_id"]));
  if (p["coupon"] != null) addEp(params, "coupon", toEpValue(p["coupon"]));
  if (p["customer_type"] != null) addEp(params, "customer_type", toEpValue(p["customer_type"]));

  for (const k of CORE_ECOMMERCE_NUMBER) {
    if (p[k] == null) continue;
    addEpn(params, k, toEpnValue(p[k]));
  }

  // Items.
  // GA4 limit: items array can include up to 200 elements.
  // https://developers.google.com/analytics/devguides/collection/ga4/ecommerce?client_type=gtag
  const itemsRaw = p["items"];
  if (Array.isArray(itemsRaw)) {
    const items = itemsRaw.slice(0, 200);
    for (let i = 0; i < items.length; i += 1) {
      const pr = encodeItemToPrValue(items[i]);
      if (pr) params.set(`pr${i + 1}`, pr);
    }
  }

  // Generic mapping: remaining scalar keys -> ep/epn based on type.
  for (const key of Object.keys(p)) {
    if (key === "items") continue;
    if (key === "currency") continue;
    if (key === "transaction_id") continue;
    if (key === "coupon") continue;
    if (key === "customer_type") continue;
    if (CORE_ECOMMERCE_NUMBER.has(key)) continue;
    // Reserved keys we handle elsewhere (config/set overrides)
    if (key === "user_id") continue;
    if (key === "client_id") continue;
    if (key === "campaign_id") continue;
    if (key === "campaign_source") continue;
    if (key === "campaign_medium") continue;
    if (key === "campaign_name") continue;
    if (key === "campaign_term") continue;
    if (key === "campaign_content") continue;
    if (key === "page_location") continue;
    if (key === "page_title") continue;
    if (key === "page_referrer") continue;
    if (key === "content_group") continue;
    if (key === "ignore_referrer") continue;
    if (key === "language") continue;
    if (key === "screen_resolution") continue;
    if (key === "cookie_domain") continue;
    if (key === "cookie_expires") continue;
    if (key === "cookie_flags") continue;
    if (key === "cookie_path") continue;
    if (key === "cookie_prefix") continue;
    if (key === "cookie_update") continue;
    if (key === "send_page_view") continue;
    if (key === "send_to") continue;

    const v = p[key];
    if (!isScalar(v)) continue;

    if (typeof v === "number") addEpn(params, key, v);
    else addEp(params, key, toEpValue(v));
  }

  return params;
}

export const __internal = {
  ITEM_KEY_MAP,
  toEpValue,
  toEpnValue,
};
