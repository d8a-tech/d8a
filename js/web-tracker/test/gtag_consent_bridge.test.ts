import test from "node:test";
import assert from "node:assert/strict";

import { createRuntimeState } from "../src/runtime/state.ts";
import { createGtagConsentBridge } from "../src/runtime/gtag_consent_bridge.ts";
import { makeWindowMock } from "./test-utils.ts";

function makeArgs(...args: unknown[]) {
  // In the real world, gtag() pushes `arguments` (array-like). For tests an array is sufficient.
  return args;
}

function makeWindow() {
  const w = makeWindowMock() as ReturnType<typeof makeWindowMock> & { dataLayer: unknown[] };
  w.dataLayer = [];
  return w;
}

test("gtag consent bridge drains existing dataLayer consent commands", () => {
  const w = makeWindow();
  const state = createRuntimeState();

  w.dataLayer.push(
    makeArgs("consent", "default", {
      ad_storage: "denied",
      analytics_storage: "denied",
      ad_user_data: "denied",
      ad_personalization: "denied",
    }),
  );

  const b = createGtagConsentBridge({ windowRef: w, getState: () => state });
  b.start();

  assert.equal(state.consentDefaultGtag.ad_storage, "denied");
  assert.equal(state.consentGtag.analytics_storage, "denied");
});

test("gtag consent bridge patches push and captures updates", () => {
  const w = makeWindow();
  const state = createRuntimeState();

  const b = createGtagConsentBridge({ windowRef: w, getState: () => state });
  b.start();

  w.dataLayer.push(
    makeArgs("consent", "update", {
      ad_storage: "granted",
      analytics_storage: "granted",
      ad_user_data: "granted",
      ad_personalization: "granted",
    }),
  );

  assert.equal(state.consentUpdateGtag.ad_storage, "granted");
  assert.equal(state.consentGtag.ad_personalization, "granted");
});

test("gtag consent bridge supports multiple instances (fan-out updates to all subscribers)", () => {
  const w = makeWindow();
  const state1 = createRuntimeState();
  const state2 = createRuntimeState();

  const b1 = createGtagConsentBridge({ windowRef: w, getState: () => state1 });
  const b2 = createGtagConsentBridge({ windowRef: w, getState: () => state2 });
  b1.start();
  b2.start();

  w.dataLayer.push(
    makeArgs("consent", "update", {
      analytics_storage: "granted",
    }),
  );

  assert.equal(state1.consentGtag.analytics_storage, "granted");
  assert.equal(state2.consentGtag.analytics_storage, "granted");
});

test("gtag consent bridge supports a custom dataLayer name", () => {
  const w = makeWindowMock() as ReturnType<typeof makeWindowMock> & { myGtagLayer: unknown[] };
  w.myGtagLayer = [];
  const state = createRuntimeState();

  w.myGtagLayer.push(
    makeArgs("consent", "update", {
      analytics_storage: "granted",
    }),
  );

  const b = createGtagConsentBridge({
    windowRef: w,
    getState: () => state,
    dataLayerName: "myGtagLayer",
  });
  b.start();

  assert.equal(state.consentGtag.analytics_storage, "granted");

  // Ensure we didn't create/patch the default dataLayer as a side-effect.
  assert.equal(w.dataLayer, undefined);
});
