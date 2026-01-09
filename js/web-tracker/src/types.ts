// Internal shared types for the web-tracker runtime.
// This file intentionally has no runtime side effects.

import type { SessionToken } from "./cookies/d8a_cookies.ts";

export type ConsentValue = "granted" | "denied";
export type ConsentKey = "ad_storage" | "analytics_storage" | "ad_user_data" | "ad_personalization";

// Consent Mode objects may contain more keys in the future; keep it flexible.
export type ConsentState = Partial<Record<ConsentKey, ConsentValue>> & Record<string, unknown>;

export type RuntimeEvent = { name: string; params: Record<string, unknown> };

export type BrowserContext = {
  dl: string; // document location
  dt: string; // document title
  dr: string; // document referrer
  dh: string; // document hostname
  ul: string; // user language
  sr: string; // screen resolution
};

export type PropertyConfig = Record<string, unknown> & {
  server_container_url?: string;
  send_page_view?: boolean;
};

export type RuntimeState = {
  jsDate: Date | null;
  pageLoadMs: number | null; // maps to GA `_p`

  // Multi-property support
  propertyIds: string[];
  propertyConfigs: Record<string, PropertyConfig>;
  primaryPropertyId: string;

  // Consent
  consent: ConsentState; // effective consent state (default + update merged)
  consentDefault: ConsentState;
  consentUpdate: ConsentState;
  consentGtag: ConsentState;
  consentDefaultGtag: ConsentState;
  consentUpdateGtag: ConsentState;

  // Identity
  userId: string | null;

  // Global defaults (gtag-style `set`)
  set: Record<string, unknown>;

  // For debugging/tests; dispatcher sends
  events: RuntimeEvent[];

  __onEvent: null | ((name: string, params: Record<string, unknown>) => void);
  __onConfig: null | ((propertyId: string, patchCfg: Record<string, unknown>) => void);

  // Request-scoped counters/timing
  hitCounter: number; // maps to `_s`
  sessionEngagementMs: number;
  sessionEngaged: boolean;

  // Cookieless (analytics_storage denied) ephemeral identity
  anonCid: string | null;

  // Cookie attribute signature tracking
  cookieAttrsSig: Record<string, string> | null;

  // Multi-property shared session state
  sharedSessionTokens: SessionToken[] | null;
  sharedSessionValue: string | null;

  // Enhanced measurement
  emInstalled: boolean;
  emSiteSearchFired: boolean;

  // Consent transition tracking (for best-effort consent-update pings)
  __lastEffectiveAnalyticsStorage: ConsentValue | undefined;
  __consentPingInitScheduled?: boolean;
  __consentPingInitDone?: boolean;
};

// Minimal "browser-like" surface used by runtime modules and tests.
// Keep this intentionally small and permissive (unknowns) to avoid DOM lib coupling,
// while still eliminating `any` in core wiring.
export type D8aTagData = Record<string, unknown> & {
  __d8aInstallResults?: Record<string, unknown>;
  __d8aInstallResultsByDataLayer?: Record<string, unknown>;
};

export type WindowLike = {
  // Optional script-tag configuration hints.
  d8aDataLayerName?: unknown;
  d8aGlobalName?: unknown;
  d8aGtagDataLayerName?: unknown;
  document: {
    cookie: string;
    title?: string;
    hidden?: boolean;
    visibilityState?: string;
    referrer?: string;
    currentScript?: unknown;
    hasFocus?: () => boolean;
    addEventListener?: (
      type: string,
      listener: (...args: unknown[]) => void,
      options?: boolean | { capture?: boolean; passive?: boolean; once?: boolean },
    ) => void;
    removeEventListener?: (
      type: string,
      listener: (...args: unknown[]) => void,
      options?: boolean | { capture?: boolean; passive?: boolean; once?: boolean },
    ) => void;
  };
  location?: {
    href?: string;
    hostname?: string;
    protocol?: string;
    search?: string;
    pathname?: string;
  };
  navigator?: {
    language?: string;
    userAgentData?: unknown;
    // Keep this assignable with real `Navigator.sendBeacon` without referencing `BodyInit`
    // (ESLint doesn't know `BodyInit` as a global in this repo).
    // Note: This is intentionally a subset of BodyInit/BufferSource.
    sendBeacon?: null | ((url: string | URL, data?: SendBeaconData) => boolean);
  };
  screen?: { width?: number; height?: number };
  addEventListener?: (
    type: string,
    listener: (...args: unknown[]) => void,
    options?: boolean | { capture?: boolean; passive?: boolean; once?: boolean },
  ) => void;
  removeEventListener?: (
    type: string,
    listener: (...args: unknown[]) => void,
    options?: boolean | { capture?: boolean; passive?: boolean; once?: boolean },
  ) => void;
  // Timers are required by the dispatcher batching logic.
  setTimeout: (handler: (...args: unknown[]) => void, timeout?: number) => number;
  clearTimeout: (id: number) => void;
  // Used to store install bookkeeping (gtag-like).
  d8a_tag_data?: D8aTagData;
  // Optional test-only helper; some tests use `w.hasFocus()`.
  hasFocus?: () => boolean;
};

export type WindowLikeTransport = {
  navigator?: {
    sendBeacon?: null | ((url: string | URL, data?: SendBeaconData) => boolean);
  };
  fetch?: (url: string, opts?: unknown) => Promise<{ status?: number }>;
};

export type SendBeaconData =
  | string
  | Blob
  | FormData
  | URLSearchParams
  | ArrayBuffer
  | null
  | undefined;
