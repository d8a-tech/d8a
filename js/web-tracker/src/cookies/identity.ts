import {
  buildD8aClientCookieValue,
  buildD8aClientCookieName,
  buildD8aCookieName,
  parseD8aClientCookie,
  parseD8aSessionCookie,
  serializeD8aSessionCookieTokens,
  updateD8aSessionCookieTokens,
} from "./d8a_cookies.ts";
import { canWriteAnalyticsCookies } from "./consent.ts";

const TWO_YEARS_SECONDS = 2 * 365 * 24 * 60 * 60;

type CookieJarLike = {
  get: (name: string) => string | undefined;
  set: (name: string, value: string, opts?: Record<string, unknown>) => unknown;
};

type ConsentStateLike = { analytics_storage?: string };

export function ensureClientId({
  jar,
  consent,
  nowMs,
  cookieDomain = "auto",
  cookiePrefix = "",
  cookiePath = "/",
  cookieSameSite = "Lax",
  cookieSecure = undefined,
  cookieMaxAgeSeconds = TWO_YEARS_SECONDS,
  cookieUpdate = true,
  forceCookieAttrsWrite = false,
}: {
  jar?: CookieJarLike;
  consent?: ConsentStateLike;
  nowMs?: number;
  cookieDomain?: string;
  cookiePrefix?: string;
  cookiePath?: string;
  cookieSameSite?: string;
  cookieSecure?: boolean;
  cookieMaxAgeSeconds?: number;
  cookieUpdate?: boolean;
  forceCookieAttrsWrite?: boolean;
} = {}) {
  const name = buildD8aClientCookieName(cookiePrefix);
  const existing = jar?.get(name);
  const parsed = parseD8aClientCookie(existing);
  // If cookie exists, we may still need to rewrite it to apply new attributes
  // (SameSite/Secure/max-age/path/domain). This is controlled by cookieUpdate.
  if (parsed?.cid) {
    if (!canWriteAnalyticsCookies(consent || {})) return { cid: parsed.cid, wrote: false };
    // For security-related changes (SameSite/Secure/etc.) we still want to be
    // able to apply updated attributes even if cookie_update is false.
    if (cookieUpdate === false && !forceCookieAttrsWrite) return { cid: parsed.cid, wrote: false };
    jar?.set(name, String(existing || ""), {
      domain: cookieDomain,
      path: cookiePath,
      sameSite: cookieSameSite,
      secure: cookieSecure,
      maxAgeSeconds: cookieMaxAgeSeconds,
    });
    return { cid: parsed.cid, wrote: true };
  }

  const newVal = buildD8aClientCookieValue({ nowMs });
  const newParsed = parseD8aClientCookie(newVal);
  const cid = newParsed?.cid || null;

  if (!canWriteAnalyticsCookies(consent || {})) {
    // Cookieless: return a generated id but do not persist.
    return { cid, wrote: false };
  }

  // Only update cookies when enabled; however, if cookie is missing, we still
  // create it even when cookieUpdate is false (gtag-like semantics).
  if (cookieUpdate === false && existing) return { cid, wrote: false };

  jar?.set(name, newVal, {
    domain: cookieDomain,
    path: cookiePath,
    sameSite: cookieSameSite,
    secure: cookieSecure,
    maxAgeSeconds: cookieMaxAgeSeconds,
  });
  return { cid, wrote: true };
}

export function ensureSession({
  jar,
  propertyId,
  consent,
  nowMs,
  cookieDomain = "auto",
  cookiePrefix = "",
  cookiePath = "/",
  cookieSameSite = "Lax",
  cookieSecure = undefined,
  cookieMaxAgeSeconds = TWO_YEARS_SECONDS,
  cookieUpdate = true,
  forceCookieAttrsWrite = false,
  sessionTimeoutMs,
}: {
  jar?: CookieJarLike;
  propertyId?: string;
  consent?: ConsentStateLike;
  nowMs?: number;
  cookieDomain?: string;
  cookiePrefix?: string;
  cookiePath?: string;
  cookieSameSite?: string;
  cookieSecure?: boolean;
  cookieMaxAgeSeconds?: number;
  cookieUpdate?: boolean;
  forceCookieAttrsWrite?: boolean;
  sessionTimeoutMs?: number;
} = {}) {
  const name = buildD8aCookieName(propertyId, cookiePrefix);
  const existing = jar?.get(name);
  const parsed = parseD8aSessionCookie(existing);

  // If cookie_update is disabled and the cookie already exists, don't refresh it.
  if (cookieUpdate === false && parsed?.tokens) {
    if (!canWriteAnalyticsCookies(consent || {})) {
      return { sid: parsed.sid, sct: parsed.sct, isNewSession: false, wrote: false };
    }
    if (!forceCookieAttrsWrite) {
      return { sid: parsed.sid, sct: parsed.sct, isNewSession: false, wrote: false };
    }
    // Apply updated cookie attributes without changing the cookie value/tokens.
    jar?.set(name, String(existing || ""), {
      domain: cookieDomain,
      path: cookiePath,
      sameSite: cookieSameSite,
      secure: cookieSecure,
      maxAgeSeconds: cookieMaxAgeSeconds,
    });
    return { sid: parsed.sid, sct: parsed.sct, isNewSession: false, wrote: true };
  }

  const updated = updateD8aSessionCookieTokens(parsed?.tokens || null, { nowMs, sessionTimeoutMs });
  const value = serializeD8aSessionCookieTokens(updated.tokens);

  const sid = Number(updated.tokens.find((t) => t.key === "s")?.val ?? NaN);
  const sct = Number(updated.tokens.find((t) => t.key === "o")?.val ?? NaN);

  const sidOk = Number.isFinite(sid) ? sid : null;
  const sctOk = Number.isFinite(sct) ? sct : null;

  if (!canWriteAnalyticsCookies(consent || {})) {
    return { sid: sidOk, sct: sctOk, isNewSession: updated.isNewSession, wrote: false };
  }

  jar?.set(name, value, {
    domain: cookieDomain,
    path: cookiePath,
    sameSite: cookieSameSite,
    secure: cookieSecure,
    maxAgeSeconds: cookieMaxAgeSeconds,
  });
  return { sid: sidOk, sct: sctOk, isNewSession: updated.isNewSession, wrote: true };
}
