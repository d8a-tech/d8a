import { stripTrailingSlashes } from "./gtag_primitives.ts";

function isProbablyUrl(s: unknown) {
  return /^https?:\/\//i.test(String(s || ""));
}

type TrackingConfigLike = { server_container_url?: string };

/**
 * Resolves the final tracking URL.
 *
 * `server_container_url` is required and is treated as the final endpoint URL.
 * Examples:
 * - Cloud: "https://global.t.d8a.tech/<property_id>/d/c"
 * - On-prem: "https://example.org/d/c"
 */
export function resolveTrackingUrl(configOrUrl?: unknown) {
  // Back-compat: allow passing a string directly (old helper signature).
  if (typeof configOrUrl === "string" && isProbablyUrl(configOrUrl)) {
    return resolveTrackingUrl({ server_container_url: configOrUrl });
  }

  const cfg =
    configOrUrl && typeof configOrUrl === "object" ? (configOrUrl as TrackingConfigLike) : {};
  const baseRaw = (cfg.server_container_url || "").trim();
  if (!baseRaw) {
    throw new Error("server_container_url is required");
  }
  return stripTrailingSlashes(baseRaw);
}

export const __internal = {
  isProbablyUrl,
};
