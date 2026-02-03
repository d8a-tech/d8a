import { version } from "../version.ts";

/** Short name for the web-tracker library (D8A client). */
const D8A_WEB_TRACKER_NAME = "wt";

/**
 * Appends D8A request metadata (_dtv, _dtn) to collect query params.
 * Call this after building GA4 params and before building the request URL.
 */
export function attachD8aCollectMetadata(params: URLSearchParams): void {
  params.set("_dtv", version);
  params.set("_dtn", D8A_WEB_TRACKER_NAME);
}
