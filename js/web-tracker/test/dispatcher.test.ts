import test from "node:test";
import assert from "node:assert/strict";

import { createDispatcher } from "../src/runtime/dispatcher.ts";
import { createRuntimeState } from "../src/runtime/state.ts";
import { isRecord } from "../src/utils/is_record.ts";
import { PROPERTY_ID, TRACKER_DOMAIN, makeWindowMock } from "./test-utils.ts";

test("dispatcher enqueues and flushes via fetch with keepalive", async () => {
  const w = makeWindowMock({
    document: {
      title: "My Dev",
      hidden: false,
      cookie: `_d8a=C1.1.457554353.1762098975; _d8a_${PROPERTY_ID}=S1.1.s1767126418$o7$g1$t1767128356$j52$dabc`,
    },
  });
  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_ID];
  state.propertyConfigs = {
    [PROPERTY_ID]: { server_container_url: `${TRACKER_DOMAIN}/d/c/${PROPERTY_ID}` },
  };

  const d = createDispatcher({ windowRef: w, getState: () => state });
  d.enqueueEvent("purchase", { value: 1, currency: "USD" });

  assert.equal(w.fetchCalls.length, 1);
  assert.match(
    w.fetchCalls[0].url,
    new RegExp(`^https://tracker\\.example\\.test/d/c/${PROPERTY_ID}\\?`),
  );
  const u = new URL(w.fetchCalls[0].url);
  assert.ok(u.searchParams.get("_dtv"), "_dtv should be set");
  assert.equal(u.searchParams.get("_dtn"), "wt", "_dtn should be web-tracker name");
  const opts = w.fetchCalls[0].opts;
  assert.ok(isRecord(opts));
  assert.equal(opts.keepalive, true);
});

test("debug_mode: adds _dbg=1 and ep.debug_mode=1 to tracking requests", async () => {
  const w = makeWindowMock();
  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_ID];
  state.propertyConfigs = {
    [PROPERTY_ID]: {
      server_container_url: `${TRACKER_DOMAIN}/d/c/${PROPERTY_ID}`,
      debug_mode: true,
    },
  };

  // Capture console.log calls without forwarding to original console.log
  const consoleLogs: unknown[][] = [];
  const originalLog = console.log;
  console.log = (...args: unknown[]) => {
    consoleLogs.push(args);
    // Don't call originalLog to suppress output during test
  };

  try {
    const d = createDispatcher({ windowRef: w, getState: () => state });
    d.enqueueEvent("page_view", {});

    assert.equal(w.fetchCalls.length, 1);
    const u = new URL(w.fetchCalls[0].url);
    assert.equal(u.searchParams.get("_dbg"), "1");
    assert.equal(u.searchParams.get("ep.debug_mode"), "1");

    // Verify that debug logs were produced
    assert.ok(consoleLogs.length > 0, "expected debug logs to be produced");
    const logMessages = consoleLogs.map((args) => String(args[0]));
    assert.ok(
      logMessages.some((msg) => msg.includes("[d8a] flush")),
      "expected [d8a] flush log message",
    );
  } finally {
    console.log = originalLog;
  }
});

test("dispatcher includes consent wire params (gcs/gcd) based on default+update states", async () => {
  const w = makeWindowMock();
  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_ID];
  state.propertyConfigs = {
    [PROPERTY_ID]: { server_container_url: `${TRACKER_DOMAIN}/d/c/${PROPERTY_ID}` },
  };
  state.consentDefault = {
    ad_storage: "denied",
    analytics_storage: "denied",
    ad_user_data: "denied",
    ad_personalization: "denied",
  };
  state.consentUpdate = {
    ad_storage: "granted",
    analytics_storage: "granted",
    ad_user_data: "granted",
    ad_personalization: "granted",
  };
  state.consent = {
    ad_storage: "granted",
    analytics_storage: "granted",
    ad_user_data: "granted",
    ad_personalization: "granted",
  };

  const d = createDispatcher({ windowRef: w, getState: () => state });
  d.enqueueEvent("page_view", {});

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("gcs"), "G111");
  assert.equal(u.searchParams.get("gcd"), "13r3r3r2r5l1");
});

test("dispatcher prefers gtag consent state (from dataLayer) over d8a consent state", async () => {
  const w = makeWindowMock();
  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_ID];
  state.propertyConfigs = {
    [PROPERTY_ID]: { server_container_url: `${TRACKER_DOMAIN}/d/c/${PROPERTY_ID}` },
  };

  // d8a consent (should be ignored if gtag consent is available)
  state.consentDefault = {
    ad_storage: "denied",
    analytics_storage: "denied",
    ad_user_data: "denied",
    ad_personalization: "denied",
  };
  state.consentUpdate = {};
  state.consent = {
    ad_storage: "denied",
    analytics_storage: "denied",
    ad_user_data: "denied",
    ad_personalization: "denied",
  };

  // gtag consent (preferred)
  state.consentDefaultGtag = {
    ad_storage: "denied",
    analytics_storage: "denied",
    ad_user_data: "denied",
    ad_personalization: "denied",
  };
  state.consentUpdateGtag = {
    ad_storage: "granted",
    analytics_storage: "granted",
    ad_user_data: "granted",
    ad_personalization: "granted",
  };
  state.consentGtag = {
    ad_storage: "granted",
    analytics_storage: "granted",
    ad_user_data: "granted",
    ad_personalization: "granted",
  };

  const d = createDispatcher({ windowRef: w, getState: () => state });
  d.enqueueEvent("page_view", {});

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("gcs"), "G111");
  assert.equal(u.searchParams.get("gcd"), "13r3r3r2r5l1");
});

test("dispatcher: analytics_storage denied => does not read cookie cid; uses ephemeral cid", async () => {
  const w = makeWindowMock();
  // Simulate an existing persistent cookie that should NOT be used when denied.
  w.document.cookie = `_d8a=C1.1.111111111.2222222222; _d8a_${PROPERTY_ID}=S1.1.s1767126418$o7$g1$t1767128356$j52$dabc`;

  // Make the cookieless cid deterministic.
  const origRandom = Math.random;
  const origNow = Date.now;
  Math.random = () => 0.123456;
  Date.now = () => 1700000000000;

  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_ID];
  state.propertyConfigs = {
    [PROPERTY_ID]: { server_container_url: `${TRACKER_DOMAIN}/d/c/${PROPERTY_ID}` },
  };
  state.consent = { analytics_storage: "denied" };
  state.consentDefault = { analytics_storage: "denied" };
  state.consentUpdate = {};

  const d = createDispatcher({ windowRef: w, getState: () => state });
  d.enqueueEvent("page_view", {});

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  // cookie cid would have been "111111111.2222222222"
  assert.notEqual(u.searchParams.get("cid"), "111111111.2222222222");
  assert.equal(u.searchParams.get("cid"), "265119741.1700000000");

  // cleanup
  Math.random = origRandom;
  Date.now = origNow;
});

test("seg flips to 1 after configured engaged time threshold and updates session cookie", async () => {
  const w = makeWindowMock();
  // Disable auto-timer flushing for deterministic test flow.
  w.setTimeout = () => 1;
  w.clearTimeout = () => {};
  // Browser-like cookie behavior: setting document.cookie appends/updates only name=value.
  const cookieStore = new Map<string, string>();
  Object.defineProperty(w.document, "cookie", {
    configurable: true,
    get: () =>
      Array.from(cookieStore.entries())
        .map(([k, v]) => `${k}=${v}`)
        .join("; "),
    set: (v: unknown) => {
      const first = String(v || "").split(";")[0] || "";
      const idx = first.indexOf("=");
      if (idx < 0) return;
      const name = first.slice(0, idx).trim();
      const value = first.slice(idx + 1).trim();
      if (!name) return;
      cookieStore.set(name, value);
    },
  });

  // Start with seg=0 in session cookie.
  cookieStore.set("_d8a", "C1.1.457554353.1762098975");
  cookieStore.set(`_d8a_${PROPERTY_ID}`, "S1.1.s1767126418$o7$g0$t1767128356$j52$dabc");

  const origNow = Date.now;
  let now = 1_000_000;
  Date.now = () => now;
  try {
    const state = createRuntimeState();
    state.propertyIds = [PROPERTY_ID];
    state.propertyConfigs = {
      // cookie_update=false normally prevents refresh; seg flip should still update value.
      [PROPERTY_ID]: {
        server_container_url: `${TRACKER_DOMAIN}/d/c/${PROPERTY_ID}`,
        cookie_update: false,
        session_engagement_time_sec: 10,
      },
    };
    state.consent = { analytics_storage: "granted" };
    state.consentDefault = {};
    state.consentUpdate = {};

    const d = createDispatcher({ windowRef: w, getState: () => state });

    // First event resets engagement timer (et=0), seg remains 0.
    d.enqueueEvent("page_view", {});
    await d.flushNow({ useBeacon: false });
    assert.equal(w.fetchCalls.length, 1);
    assert.match(w.document.cookie, /\$g0(\$|;)/);

    // 10 seconds of engaged time (focused+visible by default in this mock).
    now += 10_000;

    d.enqueueEvent("page_view", {});
    await d.flushNow({ useBeacon: false });
    assert.equal(w.fetchCalls.length, 2);

    // Cookie should be updated to g1 even when cookie_update=false.
    assert.match(w.document.cookie, /\$g1(\$|;)/);

    const u = new URL(w.fetchCalls[1].url);
    assert.equal(u.searchParams.get("seg"), "1");
  } finally {
    Date.now = origNow;
  }
});
