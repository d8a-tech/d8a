import { parseCookieHeader, createCookieJar } from "../cookies/cookie_jar.ts";
import {
  buildD8aClientCookieName,
  buildD8aCookieName,
  parseD8aClientCookie,
  parseD8aSessionCookie,
} from "../cookies/d8a_cookies.ts";
import { applyClientIdCookie, applySessionCookie } from "../cookies/identity.ts";
import { resolveCookieSettings, type CookieSettings } from "../cookies/cookie_settings.ts";
import { getEffectiveConsent } from "./consent_resolver.ts";
import { getPropertyConfig, getPropertyIds, getSetParams } from "./config_resolver.ts";
import { isRecord } from "../utils/is_record.ts";
import { getWindowSlot, setWindowSlot } from "../utils/window_slots.ts";
import type {
  IncomingDlPayload,
  LinkerConfig,
  LinkerUrlPosition,
  RuntimeState,
  WindowLike,
} from "../types.ts";

const DL_PARAM = "_dl";
const DL_VERSION = "1";

// Web-safe base64 (Base64URL alphabet, RFC 4648 §5)
const B64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
const B64_PAD = "=";
let B64_REV: Record<string, number> | null = null;
function b64Rev() {
  if (B64_REV) return B64_REV;
  const rev: Record<string, number> = {};
  for (let i = 0; i < B64_ALPHABET.length; i += 1) rev[B64_ALPHABET[i]!] = i;
  // Accept both padded and unpadded inputs, and tolerate '.' as padding for backward compatibility.
  rev[B64_PAD] = 64;
  rev["."] = 64;
  B64_REV = rev;
  return rev;
}

function utf8Encode(s: string): Uint8Array {
  try {
    if (typeof TextEncoder !== "undefined") return new TextEncoder().encode(s);
  } catch {
    // ignore
  }
  // ASCII-safe fallback (cookie values should be ASCII)
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i += 1) out[i] = s.charCodeAt(i) & 0xff;
  return out;
}

function utf8Decode(bytes: Uint8Array): string {
  try {
    if (typeof TextDecoder !== "undefined") return new TextDecoder().decode(bytes);
  } catch {
    // ignore
  }
  let s = "";
  for (let i = 0; i < bytes.length; i += 1) s += String.fromCharCode(bytes[i]!);
  return s;
}

function webSafeBase64Encode(raw: string): string {
  const bytes = utf8Encode(raw);
  const out: string[] = [];
  for (let i = 0; i < bytes.length; i += 3) {
    const has2 = i + 1 < bytes.length;
    const has3 = i + 2 < bytes.length;
    const b1 = bytes[i]!;
    const b2 = has2 ? bytes[i + 1]! : 0;
    const b3 = has3 ? bytes[i + 2]! : 0;

    const c1 = b1 >> 2;
    const c2 = ((b1 & 0x03) << 4) | (b2 >> 4);
    let c3 = ((b2 & 0x0f) << 2) | (b3 >> 6);
    let c4 = b3 & 0x3f;
    if (!has3) {
      c4 = 64; // '.'
      if (!has2) c3 = 64; // '.'
    }
    out.push(
      B64_ALPHABET[c1]!,
      B64_ALPHABET[c2]!,
      c3 === 64 ? B64_PAD : B64_ALPHABET[c3]!,
      c4 === 64 ? B64_PAD : B64_ALPHABET[c4]!,
    );
  }
  // Strip padding to keep URLs short (decoder tolerates missing padding).
  return out.join("").replace(/=+$/, "");
}

function webSafeBase64Decode(enc: string): string {
  const s = String(enc || "");
  const rev = b64Rev();
  const bytes: number[] = [];
  let i = 0;

  function next(defaultValue: number): number {
    for (; i < s.length; ) {
      const ch = s.charAt(i++);
      const v = rev[ch];
      if (v != null) return v;
      if (!/^[\s\xa0]*$/.test(ch)) throw new Error(`Invalid base64 char: ${ch}`);
    }
    return defaultValue;
  }

  for (;;) {
    const e = next(-1);
    const f = next(0);
    const g = next(64);
    const h = next(64);

    // End of input
    if (h === 64 && e === -1) break;

    bytes.push(((e << 2) | (f >> 4)) & 0xff);
    if (g !== 64) {
      bytes.push((((f << 4) & 0xf0) | (g >> 2)) & 0xff);
      if (h !== 64) bytes.push((((g << 6) & 0xc0) | h) & 0xff);
    }
  }

  return utf8Decode(new Uint8Array(bytes));
}

function isHttps(w: WindowLike) {
  try {
    return String(w?.location?.protocol || "").toLowerCase() === "https:";
  } catch {
    return false;
  }
}

function resolveLinkerConfig(cfg: LinkerConfig): Required<LinkerConfig> {
  const domains = Array.isArray(cfg?.domains)
    ? cfg.domains.filter((x) => typeof x === "string")
    : [];
  const hasDomains = domains.length > 0;
  return {
    domains,
    accept_incoming: typeof cfg?.accept_incoming === "boolean" ? cfg.accept_incoming : hasDomains,
    decorate_forms: typeof cfg?.decorate_forms === "boolean" ? cfg.decorate_forms : false,
    url_position: cfg?.url_position === "fragment" ? "fragment" : "query",
  };
}

function splitUrl(u: string) {
  // Split into base/query/fragment.
  const m = String(u || "").match(/([^?#]+)(\?[^#]*)?(#.*)?/);
  if (!m) return { base: String(u || ""), query: "", fragment: "" };
  return { base: m[1] || "", query: m[2] || "", fragment: m[3] || "" };
}

function parseParams(part: string, lead: "?" | "#") {
  const raw = String(part || "");
  const s = raw.startsWith(lead) ? raw.slice(1) : raw;
  const out: Array<[string, string]> = [];
  if (!s) return out;
  for (const kv of s.split("&")) {
    const item = kv.trim();
    if (!item) continue;
    const idx = item.indexOf("=");
    if (idx < 0) {
      out.push([decodeURIComponent(item), ""]);
      continue;
    }
    const k = decodeURIComponent(item.slice(0, idx));
    const v = decodeURIComponent(item.slice(idx + 1));
    out.push([k, v]);
  }
  return out;
}

function buildParams(pairs: Array<[string, string]>, lead: "?" | "#") {
  if (!pairs || pairs.length === 0) return "";
  const encoded = pairs
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");
  return lead + encoded;
}

function getParamFromUrl(
  url: string,
  name: string,
): { value: string | null; where: "query" | "fragment" | null } {
  const { query, fragment } = splitUrl(url);
  for (const [k, v] of parseParams(query, "?")) {
    if (k === name) return { value: v, where: "query" };
  }
  for (const [k, v] of parseParams(fragment, "#")) {
    if (k === name) return { value: v, where: "fragment" };
  }
  return { value: null, where: null };
}

function stripParamFromUrl(url: string, name: string) {
  const { base, query, fragment } = splitUrl(url);
  const q = parseParams(query, "?").filter(([k]) => k !== name);
  const f = parseParams(fragment, "#").filter(([k]) => k !== name);
  return `${base}${buildParams(q, "?")}${buildParams(f, "#")}`;
}

function setParamInUrl(url: string, name: string, value: string, pos: LinkerUrlPosition) {
  const { base, query, fragment } = splitUrl(url);
  if (pos === "fragment") {
    const f = parseParams(fragment, "#").filter(([k]) => k !== name);
    f.push([name, value]);
    return `${base}${query}${buildParams(f, "#")}`;
  }
  const q = parseParams(query, "?").filter(([k]) => k !== name);
  q.push([name, value]);
  return `${base}${buildParams(q, "?")}${fragment}`;
}

function getHostnameFromUrl(url: string, baseHref: string | undefined) {
  try {
    // Prefer URL for correctness (handles relative urls)
    const u = new URL(url, baseHref || "https://example.invalid/");
    return String(u.hostname || "");
  } catch {
    // Very small fallback: try to parse `//host/` form.
    const m = String(url || "").match(/^https?:\/\/([^/]+)/i);
    return m ? String(m[1] || "") : "";
  }
}

function shouldDecorateHost(hostname: string, domains: string[]) {
  const host = String(hostname || "");
  if (!host) return false;
  for (const d of domains) {
    const dom = String(d || "").trim();
    if (!dom) continue;
    if (host.indexOf(dom) >= 0) return true;
  }
  return false;
}

// CRC32 (base36) fingerprint hash
let CRC_TABLE: number[] | null = null;
function crcTable() {
  if (CRC_TABLE) return CRC_TABLE;
  const t: number[] = new Array(256);
  for (let i = 0; i < 256; i += 1) {
    let c = i;
    for (let j = 0; j < 8; j += 1) {
      c = c & 1 ? (c >>> 1) ^ 0xedb88320 : c >>> 1;
    }
    t[i] = c >>> 0;
  }
  CRC_TABLE = t;
  return t;
}

function crc32Base36(s: string) {
  const tbl = crcTable();
  let crc = 0xffffffff;
  for (let i = 0; i < s.length; i += 1) {
    const code = s.charCodeAt(i);
    crc = (crc >>> 8) ^ tbl[(crc ^ code) & 0xff];
  }
  const out = (crc ^ 0xffffffff) >>> 0;
  return out.toString(36);
}

function fingerprintHash({
  payload,
  offset,
  windowRef,
}: {
  payload: string;
  offset: number;
  windowRef: WindowLike;
}) {
  const ua =
    String((windowRef.navigator as any)?.userAgent || "") ||
    String((windowRef as any)?.navigator?.userAgent || "");
  const tz = new Date().getTimezoneOffset();
  const lang =
    String(windowRef.navigator?.language || "") ||
    String((windowRef.navigator as any)?.userLanguage || "");
  const minuteIndex = Math.floor(Date.now() / 60_000) - (Number(offset) || 0);
  return crc32Base36([ua, tz, lang, minuteIndex, payload].join("*"));
}

function encodeDl({
  cookies,
  ts,
  windowRef,
}: {
  cookies: Record<string, string>;
  ts: number;
  windowRef: WindowLike;
}) {
  const pairs: Array<[string, string]> = [];
  pairs.push(["ts", String(ts)]);
  for (const [k, v] of Object.entries(cookies || {})) {
    if (!k) continue;
    if (v == null) continue;
    pairs.push([k, String(v)]);
  }
  // Deterministic ordering (helps hashing + tests)
  const stable = pairs.slice(1).sort((a, b) => a[0].localeCompare(b[0]));
  const kv = [pairs[0], ...stable];
  // Encode values in web-safe base64 to avoid plaintext + reduce URL length vs percent-encoding.
  const payload = kv.map(([k, v]) => `${k}*${webSafeBase64Encode(v)}`).join("*");
  const hash = fingerprintHash({ payload, offset: 0, windowRef });
  return `${DL_VERSION}*${hash}*${payload}`;
}

function decodeDl({
  value,
  windowRef,
  ttlMs = 2 * 60 * 1000,
}: {
  value: string;
  windowRef: WindowLike;
  ttlMs?: number;
}): IncomingDlPayload | null {
  const raw = String(value || "");
  if (!raw) return null;
  const parts = raw.split("*").filter((x) => x !== "");
  if (parts.length < 5) return null;
  const version = parts[0];
  const hash = parts[1];
  if (version !== DL_VERSION) return null;

  // Reconstruct payload string as `<k>*<encodedV>*<k>*<encodedV>...`
  const payloadParts = parts.slice(2);
  const payload = payloadParts.join("*");
  let hashOk = false;
  for (let off = 0; off < 3; off += 1) {
    if (fingerprintHash({ payload, offset: off, windowRef }) === hash) {
      hashOk = true;
      break;
    }
  }
  if (!hashOk) return null;

  // Parse key/value pairs out of the payload.
  const kvRaw = payloadParts;
  const kv: Record<string, string> = {};
  for (let i = 0; i + 1 < kvRaw.length; i += 2) {
    const k = kvRaw[i];
    const encV = kvRaw[i + 1];
    if (!k) continue;
    // New format: web-safe base64, but accept legacy encodeURIComponent values for compatibility.
    try {
      kv[k] = webSafeBase64Decode(encV || "");
    } catch {
      kv[k] = decodeURIComponent(encV || "");
    }
  }
  const ts = Number(kv.ts);
  if (!Number.isFinite(ts)) return null;
  const age = Date.now() - ts;
  if (!(age >= 0 && age <= ttlMs)) return null;

  const cookies: Record<string, string> = {};
  for (const [k, v] of Object.entries(kv)) {
    if (k === "ts") continue;
    if (!k) continue;
    if (v == null) continue;
    cookies[k] = String(v);
  }
  return { ts, cookies };
}

type CookieSpec =
  | { kind: "client"; name: string; settings: CookieSettings; getState: () => RuntimeState }
  | {
      kind: "session";
      name: string;
      propertyId: string;
      settings: CookieSettings;
      getState: () => RuntimeState;
    };

const LINKER_COORDINATOR_SLOT = "__d8a_linker_coordinator";

type SharedLinkerCoordinator = ReturnType<typeof createSharedLinkerCoordinator>;

function buildCookieSpecsUnion({
  getStates,
  windowRef,
}: {
  getStates: Array<() => RuntimeState>;
  windowRef: WindowLike;
}): Record<string, CookieSpec> {
  const https = isHttps(windowRef);
  const specs: Record<string, CookieSpec> = {};

  const prefixes = new Map<string, { settings: CookieSettings; getState: () => RuntimeState }>();

  for (const getState of getStates) {
    const s = getState();
    const sp = getSetParams(getState);
    const pids = getPropertyIds(getState);
    if (pids && pids.length > 0) {
      for (const pid of pids) {
        const pcfg = getPropertyConfig(getState, pid);
        const cs = resolveCookieSettings({ propertyCfg: pcfg, setParams: sp, https, state: s });

        const sessName = buildD8aCookieName(pid, cs.cookiePrefix);
        if (!specs[sessName]) {
          specs[sessName] = {
            kind: "session",
            name: sessName,
            propertyId: pid,
            settings: cs,
            getState,
          };
        }

        if (!prefixes.has(cs.cookiePrefix))
          prefixes.set(cs.cookiePrefix, { settings: cs, getState });
      }
      continue;
    }

    // If no properties are configured yet, still include the client cookie for the current
    // cookie namespace derived from global `set` params. This matches the previous behavior
    // and allows decoration/acceptance early in the page lifecycle.
    const cs = resolveCookieSettings({ propertyCfg: {}, setParams: sp, https, state: s });
    if (!prefixes.has(cs.cookiePrefix)) prefixes.set(cs.cookiePrefix, { settings: cs, getState });
  }

  for (const [prefix, entry] of prefixes.entries()) {
    const clientName = buildD8aClientCookieName(prefix);
    if (!specs[clientName]) {
      specs[clientName] = {
        kind: "client",
        name: clientName,
        settings: entry.settings,
        getState: entry.getState,
      };
    }
  }

  return specs;
}

function createSharedLinkerCoordinator({ windowRef }: { windowRef: WindowLike }) {
  const w = windowRef;
  if (!w) throw new Error("createSharedLinkerCoordinator: windowRef is required");
  const jar = createCookieJar({ windowRef: w });

  const runtimes = new Set<() => RuntimeState>();

  function isDebugEnabled(): boolean {
    for (const gs of runtimes.values()) {
      try {
        const s = gs();
        if ((s as any)?.set && (s as any).set.debug_mode === true) return true;
        const cfgs = (s as any)?.propertyConfigs;
        if (cfgs && typeof cfgs === "object") {
          for (const v of Object.values(cfgs)) {
            if (isRecord(v) && v.debug_mode === true) return true;
          }
        }
      } catch {
        // ignore
      }
    }
    return false;
  }

  function dbg(...args: unknown[]) {
    if (!isDebugEnabled()) return;
    try {
      console.log("[d8a][linker]", ...args);
    } catch {
      // ignore
    }
  }

  // Global merged config:
  // - domains: union
  // - other fields: last-write-wins
  let rev = 0;
  const domains = new Set<string>();
  let acceptIncoming: { value: boolean; rev: number } | null = null;
  let decorateForms: { value: boolean; rev: number } | null = null;
  let urlPosition: { value: LinkerUrlPosition; rev: number } | null = null;

  // Pending incoming payload (parsed once, applied once).
  let incomingDl: IncomingDlPayload | null = null;

  let started = false;
  let formSubmitPatched = false;
  let originalFormSubmit: unknown = null;

  function getStatesList() {
    return Array.from(runtimes.values());
  }

  function currentConfig(): Required<LinkerConfig> {
    const doms = Array.from(domains.values()).sort();
    const hasDomains = doms.length > 0;
    return {
      domains: doms,
      accept_incoming: acceptIncoming ? acceptIncoming.value : hasDomains,
      decorate_forms: decorateForms ? decorateForms.value : false,
      url_position: urlPosition ? urlPosition.value : "query",
    };
  }

  function updateConfigPatch(patch: unknown) {
    if (!isRecord(patch)) return;
    rev += 1;

    const domainsRaw = patch.domains;
    if (Array.isArray(domainsRaw)) {
      for (const x of domainsRaw) {
        if (typeof x !== "string") continue;
        const v = x.trim();
        if (!v) continue;
        domains.add(v);
      }
    }

    const acceptIncomingRaw = patch.accept_incoming;
    if (typeof acceptIncomingRaw === "boolean") acceptIncoming = { value: acceptIncomingRaw, rev };

    const decorateFormsRaw = patch.decorate_forms;
    if (typeof decorateFormsRaw === "boolean") decorateForms = { value: decorateFormsRaw, rev };

    const urlPosRaw = patch.url_position;
    const urlPos = typeof urlPosRaw === "string" ? urlPosRaw : "";
    if (urlPos === "query" || urlPos === "fragment")
      urlPosition = { value: urlPos as LinkerUrlPosition, rev };
  }

  function mergeConfigSnapshot(cfg: unknown) {
    updateConfigPatch(cfg);
  }

  function registerRuntime(getState: () => RuntimeState) {
    runtimes.add(getState);
    // Best-effort: seed config from current state snapshot (helps tests and edge cases).
    try {
      mergeConfigSnapshot(getState().linker);
    } catch {
      // ignore
    }
    return () => {
      runtimes.delete(getState);
    };
  }

  function tryStripDlFromUrl(where: "query" | "fragment" | null) {
    if (!where) return;
    const href = String(w.location?.href || "");
    if (!href) return;
    const next = stripParamFromUrl(href, DL_PARAM);
    if (next !== href && typeof w.history?.replaceState === "function") {
      try {
        w.history.replaceState({}, "", next);
      } catch {
        // ignore
      }
      if (w.location) w.location.href = next;
    }
  }

  function parseIncomingFromLocation() {
    const href = String(w.location?.href || "");
    if (!href) return;
    const { value, where } = getParamFromUrl(href, DL_PARAM);
    if (!value) return;
    const decoded = decodeDl({ value, windowRef: w });
    if (decoded) {
      incomingDl = decoded;
      dbg("parsed incoming _dl", {
        cookieNames: Object.keys(decoded.cookies || {}),
        ts: decoded.ts,
        where,
      });
    }
    // Strip regardless of decode success.
    tryStripDlFromUrl(where);
  }

  function buildOutgoingCookies(): Record<string, string> {
    const specs = buildCookieSpecsUnion({ getStates: getStatesList(), windowRef: w });
    const header = String(w.document?.cookie || "");
    const all = parseCookieHeader(header);
    const out: Record<string, string> = {};
    for (const spec of Object.values(specs)) {
      const v = all.get(spec.name);
      if (!v) continue;
      if (spec.kind === "client" && !parseD8aClientCookie(v)?.cid) continue;
      if (spec.kind === "session" && !parseD8aSessionCookie(v)?.tokens) continue;
      out[spec.name] = v;
    }
    return out;
  }

  function buildDlValue(): string | null {
    const cfg = currentConfig();
    if (!cfg.domains || cfg.domains.length === 0) return null;
    const cookies = buildOutgoingCookies();
    if (!cookies || Object.keys(cookies).length === 0) return null;
    const ts = Date.now();
    return encodeDl({ cookies, ts, windowRef: w });
  }

  function decorateUrlIfNeeded(rawUrl: string): string {
    const cfg = currentConfig();
    if (!cfg.domains || cfg.domains.length === 0) return rawUrl;
    const value = buildDlValue();
    if (!value) return rawUrl;

    const host = getHostnameFromUrl(rawUrl, String(w.location?.href || ""));
    if (!shouldDecorateHost(host, cfg.domains)) return rawUrl;

    return setParamInUrl(rawUrl, DL_PARAM, value, cfg.url_position);
  }

  function decorateAnchor(el: unknown) {
    if (!isRecord(el)) return;
    const href = typeof el.href === "string" ? el.href : null;
    if (!href) return;
    const next = decorateUrlIfNeeded(href);
    if (next !== href) {
      try {
        (el as any).href = next;
      } catch {
        // ignore
      }
    }
  }

  function decorateForm(el: unknown) {
    if (!isRecord(el)) return;
    const cfg = currentConfig();
    if (!cfg.decorate_forms) return;
    const action = typeof el.action === "string" ? el.action : "";
    if (!action) return;
    const value = buildDlValue();
    if (!value) return;

    const host = getHostnameFromUrl(action, String(w.location?.href || ""));
    if (!shouldDecorateHost(host, cfg.domains)) return;

    const method = typeof el.method === "string" ? el.method.toLowerCase() : "";
    if (cfg.url_position === "query" && method === "get") {
      const children = Array.isArray((el as any).childNodes) ? (el as any).childNodes : [];
      let found = false;
      for (const c of children) {
        if (!isRecord(c)) continue;
        if (String((c as any).name || "") === DL_PARAM) {
          try {
            (c as any).value = value;
          } catch {
            // ignore
          }
          found = true;
          break;
        }
      }
      if (!found && typeof (w.document as any)?.createElement === "function") {
        try {
          const input = (w.document as any).createElement("input");
          input.setAttribute("type", "hidden");
          input.setAttribute("name", DL_PARAM);
          input.setAttribute("value", value);
          (el as any).appendChild?.(input);
        } catch {
          // ignore
        }
      }
      return;
    }

    const nextAction = setParamInUrl(action, DL_PARAM, value, cfg.url_position);
    if (nextAction !== action) {
      try {
        (el as any).action = nextAction;
      } catch {
        // ignore
      }
    }
  }

  function findClosestTarget(start: unknown, tags: string[]): unknown | null {
    let cur: any = start;
    for (let i = 0; i < 8; i += 1) {
      if (!cur || typeof cur !== "object") return null;
      const tag = String(cur.tagName || "").toUpperCase();
      if (tags.includes(tag)) return cur;
      cur = cur.parentElement || cur.parentNode || null;
    }
    return null;
  }

  function onMouseDown(ev: unknown) {
    const t = isRecord(ev) ? (ev as any).target : null;
    const a = findClosestTarget(t, ["A"]);
    if (a) decorateAnchor(a);
  }

  function onKeyUp(ev: unknown) {
    const t = isRecord(ev) ? (ev as any).target : null;
    const a = findClosestTarget(t, ["A"]);
    if (a) decorateAnchor(a);
  }

  function onSubmit(ev: unknown) {
    const t = isRecord(ev) ? (ev as any).target : null;
    const f = findClosestTarget(t, ["FORM"]);
    if (f) decorateForm(f);
  }

  function applyIncomingIfReady() {
    const cfg = currentConfig();
    if (!cfg.accept_incoming) return;
    if (!incomingDl) return;

    const getStates = getStatesList();
    const primaryGetState = getStates[0] || null;
    const consent = primaryGetState ? getEffectiveConsent(primaryGetState) : {};
    const analyticsDenied =
      String((consent as any)?.analytics_storage || "").toLowerCase() === "denied";

    if (analyticsDenied) {
      let cid: string | null = null;
      for (const v of Object.values(incomingDl.cookies)) {
        const parsed = parseD8aClientCookie(v);
        if (parsed?.cid) {
          cid = parsed.cid;
          break;
        }
      }
      if (cid) {
        for (const gs of getStates) {
          try {
            gs().anonCid = cid;
            gs().incomingDl = null;
          } catch {
            // ignore
          }
        }
      }
      incomingDl = null;
      return;
    }

    const specs = buildCookieSpecsUnion({ getStates, windowRef: w });

    // If we don't yet have enough config to recognize any cookies in the payload,
    // keep the payload around and retry later (for example, after `config()` registers
    // property IDs / cookie prefixes). This is important because `set('linker', ...)`
    // may be processed before any `config()` calls.
    let hasAnyRelevantCookie = false;
    for (const name of Object.keys(incomingDl.cookies || {})) {
      if (specs[name]) {
        hasAnyRelevantCookie = true;
        break;
      }
    }
    if (!hasAnyRelevantCookie) return;

    // Apply cookies that we can recognize *now*, but keep the rest so they can be
    // applied later when additional property configs (and thus cookie specs) become known.
    for (const [name, value] of Object.entries(incomingDl.cookies || {})) {
      const spec = specs[name];
      if (!spec) continue;
      if (spec.kind === "client") {
        const parsed = parseD8aClientCookie(value);
        if (!parsed?.cid) continue;
        const before = jar.get(spec.name);
        applyClientIdCookie({
          jar,
          cid: parsed.cid,
          consent,
          cookieDomain: spec.settings.cookieDomain,
          cookiePrefix: spec.settings.cookiePrefix,
          cookiePath: spec.settings.cookiePath,
          cookieSameSite: spec.settings.cookieSameSite,
          cookieSecure: spec.settings.cookieSecure,
          cookieMaxAgeSeconds: spec.settings.cookieMaxAgeSeconds,
          cookieUpdate: spec.settings.cookieUpdate,
          forceCookieAttrsWrite: spec.settings.forceCookieAttrsWrite,
        });
        const after = jar.get(spec.name);
        dbg("applied client cookie", {
          name: spec.name,
          cid: parsed.cid,
          cookieUpdate: spec.settings.cookieUpdate,
          before,
          after,
        });
        // Consider this cookie handled (even if cookie_update=false prevents overwriting).
        delete (incomingDl.cookies as any)[name];
        continue;
      }
      if (spec.kind === "session") {
        const s = spec.getState();
        const sp = getSetParams(spec.getState);
        const https = isHttps(w);
        const pcfg = getPropertyConfig(spec.getState, spec.propertyId);
        const cs = resolveCookieSettings({ propertyCfg: pcfg, setParams: sp, https, state: s });
        const parsed = parseD8aSessionCookie(value);
        const before = jar.get(spec.name);
        const res = applySessionCookie({
          jar,
          propertyId: spec.propertyId,
          value,
          consent,
          cookieDomain: cs.cookieDomain,
          cookiePrefix: cs.cookiePrefix,
          cookiePath: cs.cookiePath,
          cookieSameSite: cs.cookieSameSite,
          cookieSecure: cs.cookieSecure,
          cookieMaxAgeSeconds: cs.cookieMaxAgeSeconds,
          cookieUpdate: cs.cookieUpdate,
          forceCookieAttrsWrite: cs.forceCookieAttrsWrite,
        });
        const after = jar.get(spec.name);
        dbg("applied session cookie", {
          name: spec.name,
          propertyId: spec.propertyId,
          cookieUpdate: cs.cookieUpdate,
          wrote: (res as any)?.wrote === true,
          sid: parsed?.sid,
          sct: parsed?.sct,
          before,
          after,
        });
        // Seed runtime shared session state so the first auto page_view doesn't
        // overwrite an incoming linker session with a freshly generated one.
        if ((res as any)?.wrote && parsed?.tokens) {
          s.sharedSessionTokens = parsed.tokens;
          s.sharedSessionValue = String(value || "");
          dbg("seeded shared session state", { propertyId: spec.propertyId });
        }
        delete (incomingDl.cookies as any)[name];
      }
    }

    // If all cookies from the payload have been handled, clear the payload.
    if (!incomingDl.cookies || Object.keys(incomingDl.cookies).length === 0) {
      // Clear any per-runtime pending payloads after applying.
      for (const gs of getStates) {
        try {
          gs().incomingDl = null;
        } catch {
          // ignore
        }
      }
      incomingDl = null;
    }
  }

  function patchFormSubmit() {
    if (formSubmitPatched) return;
    const proto = (w as any).HTMLFormElement?.prototype;
    const submit = proto?.submit;
    if (typeof submit !== "function") return;
    originalFormSubmit = submit;
    proto.submit = function patchedSubmit(this: unknown) {
      try {
        decorateForm(this);
      } catch {
        // ignore
      }
      return submit.call(this);
    };
    formSubmitPatched = true;
  }

  function start() {
    if (started) return;
    started = true;
    parseIncomingFromLocation();
    applyIncomingIfReady();

    w.document?.addEventListener?.("mousedown", onMouseDown);
    w.document?.addEventListener?.("keyup", onKeyUp);
    w.document?.addEventListener?.("submit", onSubmit);
    patchFormSubmit();
  }

  function stop() {
    if (!started) return;
    started = false;
    w.document?.removeEventListener?.("mousedown", onMouseDown);
    w.document?.removeEventListener?.("keyup", onKeyUp);
    w.document?.removeEventListener?.("submit", onSubmit);
    if (formSubmitPatched) {
      const proto = (w as any).HTMLFormElement?.prototype;
      if (proto && originalFormSubmit) {
        try {
          proto.submit = originalFormSubmit;
        } catch {
          // ignore
        }
      }
      formSubmitPatched = false;
      originalFormSubmit = null;
    }
  }

  return {
    start,
    stop,
    applyIncomingIfReady,
    registerRuntime,
    updateConfigPatch,
    // exported for tests
    __internal: {
      encodeDl,
      decodeDl,
      decorateUrlIfNeeded,
      stripParamFromUrl,
      setParamInUrl,
      resolveLinkerConfig,
    },
  };
}

export function getSharedLinkerCoordinator({
  windowRef,
}: {
  windowRef: WindowLike;
}): SharedLinkerCoordinator {
  const w = windowRef;
  const existing = getWindowSlot<SharedLinkerCoordinator>(w, LINKER_COORDINATOR_SLOT);
  if (existing) return existing;
  const created = createSharedLinkerCoordinator({ windowRef: w });
  setWindowSlot(w, LINKER_COORDINATOR_SLOT, created);
  return created;
}

export function createLinker({
  windowRef,
  getState,
}: {
  windowRef: WindowLike;
  getState: () => RuntimeState;
}) {
  const coordinator = getSharedLinkerCoordinator({ windowRef });
  const unregister = coordinator.registerRuntime(getState);

  return {
    start: () => coordinator.start(),
    stop: () => unregister(),
    applyIncomingIfReady: () => coordinator.applyIncomingIfReady(),
    updateConfigPatch: (patch: unknown) => coordinator.updateConfigPatch(patch),
    // exported for tests
    __internal: coordinator.__internal,
  };
}
