function nowSeconds(nowMs: unknown) {
  return Math.floor((Number(nowMs ?? Date.now()) || Date.now()) / 1000);
}

function randInt31() {
  return Math.floor(Math.random() * 2147483647);
}

export function applyCookiePrefix(cookiePrefix: unknown, cookieName: unknown) {
  const p = typeof cookiePrefix === "string" ? cookiePrefix : "";
  const n = String(cookieName || "");
  if (!p) return n;
  // Avoid awkward double underscores if caller uses a trailing "_" prefix.
  if (p.endsWith("_") && n.startsWith("_")) return `${p.slice(0, -1)}${n}`;
  return `${p}${n}`;
}

export function buildD8aClientCookieName(cookiePrefix?: unknown) {
  return applyCookiePrefix(cookiePrefix, "_d8a");
}

export function buildD8aCookieName(propertyId: unknown, cookiePrefix?: unknown) {
  return applyCookiePrefix(cookiePrefix, `_d8a_${propertyId}`);
}

export function parseD8aClientCookie(value: unknown) {
  const v = String(value || "");
  // Expected: C1.1.<part1>.<part2>
  const m = v.match(/^C1\.1\.([0-9]+)\.([0-9]+)$/);
  if (!m) return null;
  return { cid: `${m[1]}.${m[2]}` };
}

/**
 * Generates the core cid parts (random + timestamp).
 * Shared by both cookie-based and anonymous cid flows.
 */
export function generateCidParts({ nowMs }: { nowMs?: unknown } = {}) {
  return {
    random: randInt31(),
    timestampSec: nowSeconds(nowMs),
  };
}

/**
 * Formats cid parts into the canonical cid string.
 */
export function formatCid({ random, timestampSec }: { random: unknown; timestampSec: unknown }) {
  return `${random}.${timestampSec}`;
}

export function buildD8aClientCookieValue({ nowMs }: { nowMs?: unknown } = {}) {
  const { random, timestampSec } = generateCidParts({ nowMs });
  // C1.1.<rand>.<timestampSeconds>
  return `C1.1.${random}.${timestampSec}`;
}

export function buildD8aClientCookieValueFromCid(cid: unknown) {
  const v = String(cid || "").trim();
  // cid format: <rand>.<timestampSeconds>
  const m = v.match(/^([0-9]+)\.([0-9]+)$/);
  if (!m) return null;
  return `C1.1.${m[1]}.${m[2]}`;
}

const SESSION_PREFIX = "S1.1.";
const KNOWN_KEYS = new Set(["s", "o", "g", "t", "j", "d"]);

export type SessionToken = { key: string; val: string; raw?: string; known: boolean };

function tokenizeSessionCookie(raw: unknown) {
  const v = String(raw || "");
  if (!v.startsWith(SESSION_PREFIX)) return null;
  const rest = v.slice(SESSION_PREFIX.length);
  const tokens = rest
    .split("$")
    .map((t) => t.trim())
    .filter(Boolean);
  const parsed: SessionToken[] = tokens.map((tok) => {
    const key = tok[0];
    const val = tok.slice(1);
    return { key, val, raw: tok, known: KNOWN_KEYS.has(key) };
  });
  return parsed;
}

function detokenizeSessionCookie(tokens: SessionToken[]) {
  const rawTokens = tokens.map((t) => `${t.key}${t.val}`);
  return `${SESSION_PREFIX}${rawTokens.join("$")}`;
}

function getToken(tokens: SessionToken[], key: string) {
  const t = tokens.find((x) => x.key === key);
  return t ? t.val : null;
}

function setToken(tokens: SessionToken[], key: string, val: unknown) {
  const idx = tokens.findIndex((x) => x.key === key);
  if (idx >= 0) tokens[idx] = { ...tokens[idx], val: String(val), known: KNOWN_KEYS.has(key) };
  else tokens.push({ key, val: String(val), known: KNOWN_KEYS.has(key) });
}

function generateOpaqueSessionId() {
  // Opaque, high-entropy-ish, URL-safe.
  const a = randInt31().toString(36);
  const b = randInt31().toString(36);
  const c = randInt31().toString(36);
  return `${a}${b}${c}`;
}

export function parseD8aSessionCookie(value: unknown) {
  const tokens = tokenizeSessionCookie(value);
  if (!tokens) return null;

  const sid = getToken(tokens, "s");
  const sct = getToken(tokens, "o");
  const last = getToken(tokens, "t");
  const segRaw = getToken(tokens, "g");

  const sidNum = sid != null ? Number(sid) : NaN;
  const sctNum = sct != null ? Number(sct) : NaN;
  const lastNum = last != null ? Number(last) : NaN;
  const segNum = segRaw != null ? Number(segRaw) : NaN;

  return {
    tokens,
    sid: Number.isFinite(sidNum) ? sidNum : null,
    sct: Number.isFinite(sctNum) ? sctNum : null,
    lastEventTs: Number.isFinite(lastNum) ? lastNum : null,
    seg: Number.isFinite(segNum) ? segNum : null,
  };
}

/**
 * Updates an existing session cookie tokens array or creates a new one.
 *
 * This is intentionally minimal in the current implementation:
 * - inactivity timeout creates a new session (new `s`, increment `o`)
 * - every event updates `t` and increments `j`
 * - preserves unknown tokens and token order where possible
 */
export function updateD8aSessionCookieTokens(
  existingTokens: SessionToken[] | null,
  { nowMs, sessionTimeoutMs }: { nowMs?: unknown; sessionTimeoutMs?: unknown } = {},
) {
  const now = nowSeconds(nowMs);
  const timeoutSec = Math.floor((Number(sessionTimeoutMs ?? 30 * 60 * 1000) || 0) / 1000);

  const isValid = Array.isArray(existingTokens) && existingTokens.length > 0;
  let tokens: SessionToken[] = isValid ? existingTokens.map((t) => ({ ...t })) : [];

  if (!isValid) {
    setToken(tokens, "s", now);
    setToken(tokens, "o", 1);
    setToken(tokens, "g", 0);
    setToken(tokens, "t", now);
    setToken(tokens, "j", 0);
    setToken(tokens, "d", generateOpaqueSessionId());
    return { tokens, isNewSession: true };
  }

  const last = Number(getToken(tokens, "t") ?? NaN);
  const lastOk = Number.isFinite(last);
  const expired = !lastOk || now - last > timeoutSec;

  if (expired) {
    const prevSct = Number(getToken(tokens, "o") ?? NaN);
    const nextSct = Number.isFinite(prevSct) ? prevSct + 1 : 1;
    setToken(tokens, "s", now);
    setToken(tokens, "o", nextSct);
    setToken(tokens, "g", 0);
    setToken(tokens, "t", now);
    setToken(tokens, "j", 0);
    setToken(tokens, "d", generateOpaqueSessionId());
    return { tokens, isNewSession: true };
  }

  // Same session.
  const prevJ = Number(getToken(tokens, "j") ?? 0);
  setToken(tokens, "j", Number.isFinite(prevJ) ? prevJ + 1 : 1);
  setToken(tokens, "t", now);
  return { tokens, isNewSession: false };
}

export function serializeD8aSessionCookieTokens(tokens: SessionToken[]) {
  return detokenizeSessionCookie(tokens);
}

export const __internal = {
  SESSION_PREFIX,
  tokenizeSessionCookie,
  detokenizeSessionCookie,
  getToken,
  setToken,
  nowSeconds,
  applyCookiePrefix,
};
