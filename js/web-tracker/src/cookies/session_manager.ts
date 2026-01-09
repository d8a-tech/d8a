import {
  buildD8aCookieName,
  parseD8aSessionCookie,
  serializeD8aSessionCookieTokens,
  updateD8aSessionCookieTokens,
  __internal as d8aCookiesInternal,
} from "./d8a_cookies.ts";
import type { PropertyConfig, RuntimeState } from "../types.ts";
import type { CookieSettings } from "./cookie_settings.ts";
import { isRecord } from "../utils/is_record.ts";

type CookieJarLike = {
  get: (name: string) => string | undefined;
  set: (
    name: string,
    value: string,
    opts?: {
      domain?: string;
      path?: string;
      sameSite?: string;
      secure?: boolean;
      maxAgeSeconds?: number;
    },
  ) =>
    | { ok?: boolean; domain?: string | null; cookieStr?: string | null; attempts?: unknown[] }
    | unknown;
};

function pickCookieSetDebugFields(res: unknown) {
  if (!isRecord(res)) return {};
  return {
    ok: res.ok,
    domain: res.domain,
    cookieStr: res.cookieStr,
    attempts: res.attempts,
  };
}

/**
 * Updates shared session tokens once per event and writes the same serialized
 * session cookie value to each destination property cookie (when allowed).
 *
 * This preserves the current dispatcher behavior while making it testable and reusable.
 */
export function updateAndWriteSharedSession({
  state,
  destinations,
  jar,
  getPropertyConfig,
  resolveCookieSettings,
  sessionTimeoutMs,
  engagementTimeMs,
  engagedThresholdMs,
  nowMs,
  log,
}: {
  state: RuntimeState;
  destinations: string[];
  jar: CookieJarLike;
  getPropertyConfig: (pid: string) => PropertyConfig;
  resolveCookieSettings: (pcfg: PropertyConfig) => CookieSettings;
  sessionTimeoutMs?: number;
  engagementTimeMs?: number;
  engagedThresholdMs?: number;
  nowMs?: number;
  log?: ((...args: unknown[]) => void) | null;
}) {
  if (!state) return;
  if (!Array.isArray(destinations) || destinations.length === 0) return;
  if (!jar) return;
  if (typeof getPropertyConfig !== "function") return;
  if (typeof resolveCookieSettings !== "function") return;

  const now = Number.isFinite(Number(nowMs)) ? Number(nowMs) : Date.now();

  let tokens = Array.isArray(state.sharedSessionTokens) ? state.sharedSessionTokens : null;
  if (!tokens) {
    // Try to parse from any existing per-property cookie.
    for (const pid of destinations) {
      const pcfg = getPropertyConfig(pid);
      const cs = resolveCookieSettings(pcfg);
      const cookieName = buildD8aCookieName(pid, cs.cookiePrefix);
      const existing = jar.get(cookieName);
      const parsed = parseD8aSessionCookie(existing);
      if (parsed?.tokens) {
        tokens = parsed.tokens;
        break;
      }
    }
  }

  const updated = updateD8aSessionCookieTokens(tokens || null, { nowMs: now, sessionTimeoutMs });

  // Track engagement within the current session and flip `g` once threshold is reached.
  if (updated.isNewSession) {
    state.sessionEngagementMs = 0;
    state.sessionEngaged = false;
  }
  const prevG = d8aCookiesInternal.getToken(updated.tokens, "g");
  state.sessionEngagementMs =
    (Number(state.sessionEngagementMs) || 0) + (Number(engagementTimeMs) || 0);
  if (
    !state.sessionEngaged &&
    Number(engagedThresholdMs) > 0 &&
    state.sessionEngagementMs >= Number(engagedThresholdMs)
  ) {
    d8aCookiesInternal.setToken(updated.tokens, "g", 1);
    state.sessionEngaged = true;
  }
  const nextG = d8aCookiesInternal.getToken(updated.tokens, "g");
  const shouldWriteSessionValue = updated.isNewSession || prevG !== nextG;

  state.sharedSessionTokens = updated.tokens;
  state.sharedSessionValue = serializeD8aSessionCookieTokens(updated.tokens);

  // Write the same session cookie value to every destination property cookie.
  for (const pid of destinations) {
    const pcfg = getPropertyConfig(pid);
    const cs = resolveCookieSettings(pcfg);
    const cookieName = buildD8aCookieName(pid, cs.cookiePrefix);
    const existing = jar.get(cookieName);

    if (!existing) {
      // Always create missing cookie (recreation behavior).
      const res = jar.set(cookieName, state.sharedSessionValue, {
        domain: cs.cookieDomain,
        path: cs.cookiePath,
        sameSite: cs.cookieSameSite,
        secure: cs.cookieSecure,
        maxAgeSeconds: cs.cookieMaxAgeSeconds ?? undefined,
      });
      if (cs.debug)
        log?.("[d8a] set session cookie (create)", {
          pid,
          cookieName,
          ...pickCookieSetDebugFields(res),
          cookieReadBack: jar.get(cookieName),
        });
      continue;
    }

    if (cs.cookieUpdate === true || shouldWriteSessionValue) {
      const res = jar.set(cookieName, state.sharedSessionValue, {
        domain: cs.cookieDomain,
        path: cs.cookiePath,
        sameSite: cs.cookieSameSite,
        secure: cs.cookieSecure,
        maxAgeSeconds: cs.cookieMaxAgeSeconds ?? undefined,
      });
      if (cs.debug)
        log?.("[d8a] set session cookie (value+attrs update)", {
          pid,
          cookieName,
          ...pickCookieSetDebugFields(res),
          cookieReadBack: jar.get(cookieName),
        });
      continue;
    }

    if (cs.forceCookieAttrsWrite) {
      // Security attribute change: rewrite attrs without changing value when cookie_update=false.
      const res = jar.set(cookieName, existing, {
        domain: cs.cookieDomain,
        path: cs.cookiePath,
        sameSite: cs.cookieSameSite,
        secure: cs.cookieSecure,
        maxAgeSeconds: cs.cookieMaxAgeSeconds ?? undefined,
      });
      if (cs.debug)
        log?.("[d8a] set session cookie (attrs update)", {
          pid,
          cookieName,
          ...pickCookieSetDebugFields(res),
          cookieReadBack: jar.get(cookieName),
        });
      continue;
    }

    if (cs.debug) {
      log?.("[d8a] set session cookie (no write)", {
        pid,
        cookieName,
        reason:
          "cookie_update=false and session value unchanged and force_cookie_attrs_write=false",
        cookieUpdate: cs.cookieUpdate,
        shouldWriteSessionValue,
        forceCookieAttrsWrite: cs.forceCookieAttrsWrite,
        cookieReadBack: jar.get(cookieName),
      });
    }
  }
}
