export type ConsentModeAction = "default" | "update";
export type ConsentStatus = "granted" | "denied";

export type ConsentState = {
  ad_storage?: ConsentStatus;
  analytics_storage?: ConsentStatus;
  functionality_storage?: ConsentStatus;
  personalization_storage?: ConsentStatus;
  security_storage?: ConsentStatus;
};

export type D8aConfigParams = {
  /**
   * Server-side destination URL (tracker uses this as the final endpoint).
   * Example (cloud): "https://global.t.d8a.tech/<property_id>/d/c"
   * Example (on-prem): "https://example.org/d/c"
   */
  server_container_url?: string;

  /**
   * Optional name of the queue array on `window` used for buffering calls.
   * For script-tag usage, you can also pass it via the script src `?l=<name>`.
   */
  data_layer_name?: string;
  debug_mode?: boolean;
  cookie_domain?: "auto" | "none" | string;
  cookie_path?: string;
  cookie_expires?: number;
  cookie_flags?: string;
  cookie_prefix?: string;
  cookie_update?: boolean;
  session_timeout_ms?: number;
  /**
   * Minimum engaged time (in seconds) required to flip `seg=1` (session engaged).
   * Defaults to 10.
   */
  session_engagement_time_sec?: number;
  flush_interval_ms?: number;
  max_batch_size?: number;

  /**
   * GA4-style user id (stored in-memory by the tracker). On the wire it maps
   * to the `uid` parameter.
   */
  user_id?: string;

  // gtag-style request/page overrides
  client_id?: string;
  campaign_id?: string;
  campaign_source?: string;
  campaign_medium?: string;
  campaign_name?: string;
  campaign_term?: string;
  campaign_content?: string;
  page_location?: string;
  page_title?: string;
  page_referrer?: string;
  content_group?: string;
  language?: string;
  screen_resolution?: string;
  ignore_referrer?: boolean;
  send_page_view?: boolean;

  // Enhanced measurement
  site_search_enabled?: boolean;
  site_search_query_params?: string | string[];
  outbound_clicks_enabled?: boolean;
  outbound_exclude_domains?: string | string[];
  file_downloads_enabled?: boolean;
  file_download_extensions?: string | string[];
};

export type D8aSetParams = {
  user_id?: string;

  // same override keys as config (global defaults)
  client_id?: string;
  campaign_id?: string;
  campaign_source?: string;
  campaign_medium?: string;
  campaign_name?: string;
  campaign_term?: string;
  campaign_content?: string;
  page_location?: string;
  page_title?: string;
  page_referrer?: string;
  content_group?: string;
  language?: string;
  screen_resolution?: string;
  ignore_referrer?: boolean;

  cookie_domain?: "auto" | "none" | string;
  cookie_path?: string;
  cookie_expires?: number;
  cookie_flags?: string;
  cookie_prefix?: string;
  cookie_update?: boolean;
  /**
   * Global default for `session_engagement_time_sec` (see config).
   */
  session_engagement_time_sec?: number;

  // Enhanced measurement
  site_search_enabled?: boolean;
  site_search_query_params?: string | string[];
  outbound_clicks_enabled?: boolean;
  outbound_exclude_domains?: string | string[];
  file_downloads_enabled?: boolean;
  file_download_extensions?: string | string[];
};

export type D8aEventParams = Record<
  string,
  string | number | boolean | null | undefined | string[] | Array<Record<string, unknown>>
>;

export interface D8aFn {
  (...args: unknown[]): void;
  js(date: Date): void;
  config(propertyId: string, params?: D8aConfigParams): void;
  event(eventName: string, params?: D8aEventParams): void;
  set(params: D8aSetParams): void;
  consent(action: ConsentModeAction, state: ConsentState): void;
}

declare global {
  interface Window {
    dataLayer?: unknown[];
    d8a?: D8aFn;
    d8aDataLayerName?: string;
    d8aGlobalName?: string;
    d8aGtagDataLayerName?: string;
  }
}

export type InstallD8aOptions = {
  windowRef?: unknown;
  dataLayerName?: string;
  globalName?: string;
  gtagDataLayerName?: string;
};

export type QueueConsumer = {
  start(): void;
  stop(): void;
  getState(): unknown;
  setOnEvent(fn: (name: string, params: D8aEventParams) => void): void;
  setOnConfig(fn: (propertyId: string, params: D8aConfigParams) => void): void;
};

export type Dispatcher = {
  enqueueEvent(name: string, params?: D8aEventParams): void;
  flush(opts: { useBeacon: boolean }): Promise<{ sent: number }>;
  flushNow(opts?: { useBeacon?: boolean }): Promise<{ sent: number }>;
  attachLifecycleFlush(): void;
};

export type InstallD8aResult = {
  consumer: QueueConsumer;
  dispatcher: Dispatcher;
  dataLayerName: string;
  globalName: string;
};

export function installD8a(opts?: InstallD8aOptions): InstallD8aResult;

export {};
