/**
 * Small helpers used to implement a gtag-compatible integration surface.
 *
 * Important: this code is a clean reimplementation of behaviors we need for compatibility
 * (e.g. cookie domain candidate ordering).
 */

export function getCookieDomainCandidates(hostname: unknown) {
  const host = String(hostname || "").trim();
  if (!host) return ["none"];

  const parts = host.split(".");

  // IP address detection: treat IPv4-like hostnames as host-only cookie candidates.
  if (parts.length === 4) {
    const last = parts[parts.length - 1];
    if (String(Number(last)) === last) return ["none"];
  }

  // Broadest-to-narrowest to prefer the widest cookie domain that still works.
  // For docs.d8a.tech => ["d8a.tech", "docs.d8a.tech", "none"]
  const out: string[] = [];
  for (let i = parts.length - 2; i >= 0; i -= 1) {
    out.push(parts.slice(i).join("."));
  }
  out.push("none");
  return out;
}

export function stripTrailingSlashes(s: unknown) {
  return String(s).replace(/\/+$/, "");
}
