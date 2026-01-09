type CookieFlagsParsed = { secure?: boolean; sameSite?: "Lax" | "Strict" | "None" };

function parseCookieFlags(cookieFlags: unknown): CookieFlagsParsed {
  const raw = typeof cookieFlags === "string" ? cookieFlags : "";
  if (!raw.trim()) return {};
  const parts = raw
    .split(";")
    .map((s) => s.trim())
    .filter(Boolean);
  const out: CookieFlagsParsed = {};
  for (const p of parts) {
    const lower = p.toLowerCase();
    if (lower === "secure") out.secure = true;
    if (lower.startsWith("samesite=")) {
      const vRaw = p.split("=").slice(1).join("=").trim();
      const v = vRaw.toLowerCase();
      if (v === "lax") out.sameSite = "Lax";
      if (v === "strict") out.sameSite = "Strict";
      if (v === "none") out.sameSite = "None";
    }
  }
  return out;
}

type DebugEnabledFn = (args: {
  config: Record<string, unknown>;
  setParams: Record<string, unknown>;
}) => boolean;

import { isRecord } from "../utils/is_record.ts";

export type CookieSettings = {
  cookieDomain: string;
  cookiePrefix: string;
  cookiePath: string;
  cookieSameSite: "Lax" | "Strict" | "None";
  cookieSecure: boolean;
  cookieUpdate: boolean;
  cookieMaxAgeSeconds: number;
  forceCookieAttrsWrite: boolean;
  debug: boolean;
};

export function resolveCookieSettings({
  propertyCfg,
  setParams,
  https,
  state,
  isDebugEnabled,
}: {
  propertyCfg?: import("../types.ts").PropertyConfig;
  setParams?: Record<string, unknown>;
  https?: boolean;
  state?: import("../types.ts").RuntimeState;
  isDebugEnabled?: DebugEnabledFn;
}): CookieSettings {
  const cfg: Record<string, unknown> = isRecord(propertyCfg) ? propertyCfg : {};
  const sp: Record<string, unknown> = isRecord(setParams) ? setParams : {};

  const cookieDomainRaw = cfg.cookie_domain ?? sp.cookie_domain;
  const cookieDomain =
    typeof cookieDomainRaw === "string" && cookieDomainRaw.trim()
      ? cookieDomainRaw
      : cookieDomainRaw == null
        ? "auto"
        : String(cookieDomainRaw);

  const cookiePrefixRaw = cfg.cookie_prefix ?? sp.cookie_prefix;
  const cookiePrefix = typeof cookiePrefixRaw === "string" ? cookiePrefixRaw : "";

  const cookiePathRaw = cfg.cookie_path ?? sp.cookie_path;
  const cookiePath =
    typeof cookiePathRaw === "string" && cookiePathRaw.trim() ? cookiePathRaw : "/";

  const cookieUpdateRaw = cfg.cookie_update ?? sp.cookie_update;
  const cookieUpdate = typeof cookieUpdateRaw === "boolean" ? cookieUpdateRaw : true;

  // NOTE: Do not default this to null; Number(null) === 0 which would set Max-Age=0
  // and immediately delete cookies in browsers.
  const cookieExpires = cfg.cookie_expires ?? sp.cookie_expires;
  const parsedExpires = Number.isFinite(Number(cookieExpires)) ? Number(cookieExpires) : null;
  // GA4/gtag-like default lifetime: 2 years for both client + per-property cookies.
  // (The per-property cookie contains session state, but the cookie itself persists.)
  const cookieMaxAgeSeconds = parsedExpires != null ? parsedExpires : 2 * 365 * 24 * 60 * 60;

  const flagsRaw = cfg.cookie_flags ?? sp.cookie_flags ?? "";
  const flags = parseCookieFlags(flagsRaw);
  const cookieSameSite: "Lax" | "Strict" | "None" = flags.sameSite || "Lax";
  // Cookie flags can request Secure, but we never set Secure on non-HTTPS pages.
  const cookieSecure = https === true && flags.secure === true ? true : https === true;
  const debug =
    typeof isDebugEnabled === "function" ? isDebugEnabled({ config: cfg, setParams: sp }) : false;

  // Detect attribute changes so we can apply security changes even when cookie_update=false.
  const sig = JSON.stringify({
    domain: cookieDomain,
    prefix: cookiePrefix,
    path: cookiePath,
    sameSite: cookieSameSite,
    secure: cookieSecure,
    maxAgeSeconds: cookieMaxAgeSeconds,
  });
  const sigKey = `${cookiePrefix}|${cookieDomain}|${cookiePath}`;
  if (state && !state.cookieAttrsSig) state.cookieAttrsSig = {};
  const prev = state?.cookieAttrsSig ? state.cookieAttrsSig[sigKey] : undefined;
  const forceCookieAttrsWrite = prev !== sig;
  if (state?.cookieAttrsSig) state.cookieAttrsSig[sigKey] = sig;

  return {
    cookieDomain,
    cookiePrefix,
    cookiePath,
    cookieSameSite,
    cookieSecure,
    cookieUpdate,
    cookieMaxAgeSeconds,
    forceCookieAttrsWrite,
    debug,
  };
}
