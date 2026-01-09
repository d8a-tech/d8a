function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function shouldRetryHttpStatus(status: number) {
  if (status === 429) return true;
  if (status >= 500) return true;
  return false;
}

import type { WindowLikeTransport } from "../types.ts";

export async function sendWithRetries({
  url,
  windowRef,
  useBeacon,
  maxRetries = 1,
  initialBackoffMs = 200,
}: {
  url: string;
  windowRef: WindowLikeTransport;
  useBeacon: boolean;
  maxRetries?: number;
  initialBackoffMs?: number;
}) {
  const w = windowRef;
  const beacon = w?.navigator?.sendBeacon;

  if (useBeacon && typeof beacon === "function") {
    // sendBeacon doesn't give us status, so it's best-effort.
    try {
      beacon.call(w.navigator, url);
      return { ok: true, via: "beacon" as const };
    } catch {
      // fall back to fetch path
    }
  }

  const fetchFn = w?.fetch;
  if (typeof fetchFn !== "function") {
    throw new Error("fetch is not available");
  }

  const options = {
    method: "POST",
    keepalive: true,
    credentials: "include",
    cache: "no-store",
    redirect: "follow",
    mode: "no-cors",
  } as const;

  let attempt = 0;
  let backoff = initialBackoffMs;
  while (true) {
    attempt += 1;
    try {
      const res = await fetchFn.call(w, url, options);

      // With `no-cors`, response is often opaque (status 0). Treat as success.
      const status = typeof res?.status === "number" ? res.status : 0;
      if (!shouldRetryHttpStatus(status)) {
        return { ok: true, via: "fetch" as const, status };
      }
    } catch {
      // retry below
    }

    if (attempt > maxRetries) {
      return { ok: false, via: "fetch" as const, status: null };
    }
    await sleep(backoff);
    backoff *= 2;
  }
}
