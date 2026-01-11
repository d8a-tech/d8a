import { getCookieDomainCandidates } from "../utils/gtag_primitives.ts";

type CookieMap = Map<string, string>;

type DocumentLike = {
  cookie: string;
};

type WindowRefLike = {
  document?: DocumentLike;
  location?: { protocol?: string; hostname?: string };
};

type CookieSetOptions = {
  path?: string;
  domain?: string;
  sameSite?: string;
  secure?: boolean;
  maxAgeSeconds?: number | null;
  expires?: Date | null;
};

type CookieSetAttempt = {
  candidate: string;
  domain: string | null;
  cookieStr: string;
  before: string;
  after: string;
  readBack: string | undefined;
  stuck: boolean;
};

type CookieSetResult = {
  ok: boolean;
  domain: string | null;
  cookieStr: string | null;
  attempts: CookieSetAttempt[];
};

function parseCookieHeader(header: unknown): CookieMap {
  const out: CookieMap = new Map();
  const raw = String(header || "");
  if (!raw) return out;
  for (const part of raw.split(";")) {
    const s = part.trim();
    if (!s) continue;
    const idx = s.indexOf("=");
    if (idx < 0) continue;
    const name = s.slice(0, idx).trim();
    const value = s.slice(idx + 1).trim();
    if (!name) continue;
    out.set(name, value);
  }
  return out;
}

export { parseCookieHeader };

function isHttps(w: WindowRefLike) {
  try {
    return String(w?.location?.protocol || "").toLowerCase() === "https:";
  } catch {
    return false;
  }
}

function buildSetCookieString(name: string, value: string, attrs: Record<string, unknown>) {
  const parts: string[] = [];
  parts.push(`${name}=${value}`);
  if (attrs.path) parts.push(`Path=${String(attrs.path)}`);
  if (attrs.expires) parts.push(`Expires=${(attrs.expires as Date).toUTCString()}`);
  if (attrs.maxAgeSeconds != null) parts.push(`Max-Age=${String(attrs.maxAgeSeconds)}`);
  if (attrs.sameSite) parts.push(`SameSite=${String(attrs.sameSite)}`);
  if (attrs.domain) parts.push(`Domain=${String(attrs.domain)}`);
  if (attrs.secure) parts.push("Secure");
  return parts.join("; ");
}

function ensureDotDomain(domain: string) {
  if (!domain) return domain;
  if (domain === "none") return null;
  return domain.startsWith(".") ? domain : `.${domain}`;
}

export function createCookieJar({ windowRef }: { windowRef: WindowRefLike }) {
  const w = windowRef;
  if (!w?.document) throw new Error("createCookieJar: windowRef.document is required");
  const doc = w.document;

  function getAll() {
    return parseCookieHeader(w.document?.cookie);
  }

  function get(name: string) {
    return getAll().get(name);
  }

  /**
   * Sets a cookie, optionally using `domain: 'auto'` to select a
   * domain that sticks by trying candidates broadest-first.
   *
   * Returns an object describing what it did; primarily useful for debugging/tests.
   */
  function set(name: string, value: string, opts: CookieSetOptions = {}): CookieSetResult {
    const path = opts.path || "/";
    const https = isHttps(w);
    // Browsers require Secure for SameSite=None cookies. On http://, we can't set
    // Secure cookies, so we downgrade SameSite=None to Lax for local dev.
    const requestedSameSite = opts.sameSite || "Lax";
    const sameSite =
      !https && String(requestedSameSite).toLowerCase() === "none" ? "Lax" : requestedSameSite;
    // Browsers won't accept `Secure` cookies on non-HTTPS pages. For local dev
    // (http://), we ignore an explicit Secure request rather than failing to
    // set/update the cookie at all.
    const secure = opts.secure != null ? !!opts.secure && https : https;
    const maxAgeSeconds = opts.maxAgeSeconds ?? null;
    const expires = opts.expires ?? null;

    const baseAttrs = { path, sameSite, secure, maxAgeSeconds, expires };

    const domainOpt = opts.domain || "auto";
    const host = String(w.location?.hostname || "");
    const candidates = domainOpt === "auto" ? getCookieDomainCandidates(host) : [domainOpt];

    // gtag semantics: "none" means host-only (no domain attribute).
    const attempts: CookieSetAttempt[] = [];
    for (const candidate of candidates) {
      const domain = ensureDotDomain(candidate);
      const cookieStr = buildSetCookieString(name, value, { ...baseAttrs, domain });

      const before = String(doc.cookie || "");
      doc.cookie = cookieStr;
      const after = String(doc.cookie || "");

      const stuck = before !== after || get(name) === value;
      attempts.push({
        candidate,
        domain: candidate === "none" ? null : domain,
        cookieStr,
        before,
        after,
        readBack: get(name),
        stuck,
      });
      if (stuck) {
        return { ok: true, domain: candidate === "none" ? null : domain, cookieStr, attempts };
      }
    }

    return { ok: false, domain: null, cookieStr: null, attempts };
  }

  function del(name: string, opts: CookieSetOptions = {}) {
    // Delete by setting an expired cookie; attempt with same domain selection.
    return set(name, "deleted", { ...opts, expires: new Date(0) });
  }

  return { get, set, del, __internal: { parseCookieHeader, buildSetCookieString } };
}
