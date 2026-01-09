import test from "node:test";
import assert from "node:assert/strict";

import { createDispatcher } from "../src/runtime/dispatcher.ts";
import { createRuntimeState } from "../src/runtime/state.ts";
import { PROPERTY_ID, makeWindowMock, tick } from "./test-utils.ts";

function makeWindow() {
  return makeWindowMock({
    location: { href: "http://example.test/", hostname: "example.test", protocol: "http:" },
    navigator: { language: "en-GB" },
    screen: { width: 100, height: 100 },
    document: {
      title: "T",
      referrer: "",
      cookie: "",
      hidden: false,
      hasFocus: () => true,
    },
    hasFocus: () => true,
    setTimeout: (fn: () => void) => {
      // run timers ASAP in tests
      Promise.resolve().then(() => fn());
      return 1;
    },
    clearTimeout: () => {},
  });
}

test("user_engagement is emitted on visibilitychange(hidden) with _et equal to focused+visible time since last reset", async () => {
  const w = makeWindow();

  // Deterministic time control.
  const realNow = Date.now;
  let now = 1_000;
  Date.now = () => now;
  try {
    const state = createRuntimeState();
    state.propertyIds = [PROPERTY_ID];
    state.propertyConfigs = {
      [PROPERTY_ID]: { server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c` },
    };
    state.consent = { analytics_storage: "granted" };
    state.consentDefault = {};
    state.consentUpdate = {};

    const dispatcher = createDispatcher({ windowRef: w, getState: () => state });
    dispatcher.attachLifecycleFlush();

    // Send a first event to reset the engagement timer.
    dispatcher.enqueueEvent("page_view", {});
    await tick();
    await dispatcher.flushNow({ useBeacon: false });
    assert.equal(w.fetchCalls.length, 1);
    {
      const u = new URL(w.fetchCalls[0].url);
      assert.equal(u.searchParams.get("en"), "page_view");
      assert.equal(u.searchParams.get("_et"), "0");
    }

    // Accumulate 11 seconds of engaged time (focused + visible).
    now += 11_000;

    // Hide tab: should enqueue + flush user_engagement.
    w.document.hidden = true;
    const vcs = w.__docListeners.get("visibilitychange") || [];
    assert.ok(vcs.length > 0);
    for (const fn of vcs) fn();
    await tick();

    assert.equal(w.fetchCalls.length, 2);
    const u2 = new URL(w.fetchCalls[1].url);
    assert.equal(u2.searchParams.get("en"), "user_engagement");
    assert.equal(u2.searchParams.get("_et"), "11000");
  } finally {
    Date.now = realNow;
  }
});

test("user_engagement is NOT emitted on visibilitychange(hidden) for short tab switches (<10s)", async () => {
  const w = makeWindow();

  const realNow = Date.now;
  let now = 1_000;
  Date.now = () => now;
  try {
    const state = createRuntimeState();
    state.propertyIds = [PROPERTY_ID];
    state.propertyConfigs = {
      [PROPERTY_ID]: { server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c` },
    };
    state.consent = { analytics_storage: "granted" };
    state.consentDefault = {};
    state.consentUpdate = {};

    const dispatcher = createDispatcher({ windowRef: w, getState: () => state });
    dispatcher.attachLifecycleFlush();

    dispatcher.enqueueEvent("page_view", {});
    await tick();
    await dispatcher.flushNow({ useBeacon: false });
    assert.equal(w.fetchCalls.length, 1);

    // Accumulate only 9 seconds.
    now += 9_000;

    w.document.hidden = true;
    const vcs = w.__docListeners.get("visibilitychange") || [];
    assert.ok(vcs.length > 0);
    for (const fn of vcs) fn();
    await tick();

    // Still only the original page_view should have been sent.
    assert.equal(w.fetchCalls.length, 1);
  } finally {
    Date.now = realNow;
  }
});

test("user_engagement is emitted on pagehide with any accumulated engagement time", async () => {
  const w = makeWindow();

  const realNow = Date.now;
  let now = 1_000;
  Date.now = () => now;
  try {
    const state = createRuntimeState();
    state.propertyIds = [PROPERTY_ID];
    state.propertyConfigs = {
      [PROPERTY_ID]: { server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c` },
    };
    state.consent = { analytics_storage: "granted" };
    state.consentDefault = {};
    state.consentUpdate = {};

    const dispatcher = createDispatcher({ windowRef: w, getState: () => state });
    dispatcher.attachLifecycleFlush();

    // Send a first event to reset the engagement timer.
    dispatcher.enqueueEvent("page_view", {});
    await tick();
    await dispatcher.flushNow({ useBeacon: false });
    assert.equal(w.fetchCalls.length, 1);

    // Accumulate 5 seconds of engaged time (less than 10s threshold).
    now += 5_000;

    // Trigger pagehide: should enqueue + flush user_engagement even with <10s.
    const phs = w.__listeners.get("pagehide") || [];
    assert.ok(phs.length > 0);
    for (const fn of phs) fn();
    await tick();

    assert.equal(w.fetchCalls.length, 2);
    const u = new URL(w.fetchCalls[1].url);
    assert.equal(u.searchParams.get("en"), "user_engagement");
    assert.equal(u.searchParams.get("_et"), "5000");
  } finally {
    Date.now = realNow;
  }
});

test("user_engagement is NOT emitted on pagehide when there's no engagement time", async () => {
  const w = makeWindow();

  const realNow = Date.now;
  let now = 1_000;
  Date.now = () => now;
  try {
    const state = createRuntimeState();
    state.propertyIds = [PROPERTY_ID];
    state.propertyConfigs = {
      [PROPERTY_ID]: { server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c` },
    };
    state.consent = { analytics_storage: "granted" };
    state.consentDefault = {};
    state.consentUpdate = {};

    const dispatcher = createDispatcher({ windowRef: w, getState: () => state });
    dispatcher.attachLifecycleFlush();

    // Send a first event to reset the engagement timer.
    dispatcher.enqueueEvent("page_view", {});
    await tick();
    await dispatcher.flushNow({ useBeacon: false });
    assert.equal(w.fetchCalls.length, 1);

    // Don't accumulate any time - immediately trigger pagehide.
    // (engagement timer was reset after the first event, so peek() should be 0)
    const phs = w.__listeners.get("pagehide") || [];
    assert.ok(phs.length > 0);
    for (const fn of phs) fn();
    await tick();

    // Should only have the original page_view, no user_engagement.
    assert.equal(w.fetchCalls.length, 1);
  } finally {
    Date.now = realNow;
  }
});

test("user_engagement is emitted on pagehide with accumulated engagement time even if <10s threshold", async () => {
  const w = makeWindow();

  const realNow = Date.now;
  let now = 1_000;
  Date.now = () => now;
  try {
    const state = createRuntimeState();
    state.propertyIds = [PROPERTY_ID];
    state.propertyConfigs = {
      [PROPERTY_ID]: { server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c` },
    };
    state.consent = { analytics_storage: "granted" };
    state.consentDefault = {};
    state.consentUpdate = {};

    const dispatcher = createDispatcher({ windowRef: w, getState: () => state });
    dispatcher.attachLifecycleFlush();

    // Send a first event to reset the engagement timer.
    dispatcher.enqueueEvent("page_view", {});
    await tick();
    await dispatcher.flushNow({ useBeacon: false });
    assert.equal(w.fetchCalls.length, 1);

    // Accumulate only 1 second (much less than 10s threshold).
    now += 1_000;

    // Trigger pagehide: should still fire user_engagement (unlike visibilitychange).
    const phs = w.__listeners.get("pagehide") || [];
    assert.ok(phs.length > 0);
    for (const fn of phs) fn();
    await tick();

    assert.equal(w.fetchCalls.length, 2);
    const u = new URL(w.fetchCalls[1].url);
    assert.equal(u.searchParams.get("en"), "user_engagement");
    assert.equal(u.searchParams.get("_et"), "1000");
  } finally {
    Date.now = realNow;
  }
});
