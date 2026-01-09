import test from "node:test";
import assert from "node:assert/strict";

import { createRuntimeState } from "../src/runtime/state.ts";
import { createQueueConsumer } from "../src/runtime/queue_consumer.ts";
import { createGtagConsentBridge } from "../src/runtime/gtag_consent_bridge.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { PROPERTY_ID, makeWindowMock, tick } from "./test-utils.ts";

function makeArgs(...args: unknown[]) {
  // In the real world, gtag()/snippet pushes `arguments` (array-like). For tests an array is sufficient.
  return args;
}

test("does not emit a consent-update ping when analytics_storage default->update happens in the same tick (d8a consent)", async () => {
  const w = makeWindowMock();
  const d8aLayer = ensureArraySlot<unknown>(w, "d8aLayer");
  const events: Array<{ name: string; params: unknown }> = [];

  const consumer = createQueueConsumer({ windowRef: w, dataLayerName: "d8aLayer" });
  consumer.setOnEvent((name, params) => events.push({ name, params }));
  consumer.start();

  // Ensure at least one property is configured; disable auto page_view for a clean assertion.
  d8aLayer.push(makeArgs("config", PROPERTY_ID, { send_page_view: false }));

  // Initialize then transition.
  d8aLayer.push(makeArgs("consent", "default", { analytics_storage: "denied" }));
  d8aLayer.push(makeArgs("consent", "update", { analytics_storage: "granted" }));

  await tick();
  assert.deepEqual(events, []);
});

test("emits a consent-update ping when analytics_storage changes after the init tick (d8a consent)", async () => {
  const w = makeWindowMock();
  const d8aLayer = ensureArraySlot<unknown>(w, "d8aLayer");
  const events: Array<{ name: string; params: unknown }> = [];

  const consumer = createQueueConsumer({ windowRef: w, dataLayerName: "d8aLayer" });
  consumer.setOnEvent((name, params) => events.push({ name, params }));
  consumer.start();

  d8aLayer.push(makeArgs("config", PROPERTY_ID, { send_page_view: false }));

  d8aLayer.push(makeArgs("consent", "default", { analytics_storage: "denied" }));
  await tick();
  d8aLayer.push(makeArgs("consent", "update", { analytics_storage: "granted" }));

  assert.deepEqual(events, [{ name: "user_engagement", params: { consent_update: 1 } }]);
});

test("does not emit a consent-update ping when analytics_storage default->update happens in the same tick (mirrored gtag consent)", async () => {
  const w = makeWindowMock();
  const dataLayer = ensureArraySlot<unknown>(w, "dataLayer");
  const state = createRuntimeState();
  const events: Array<{ name: string; params: unknown }> = [];

  // Simulate already-configured property (we drop the ping before config).
  state.propertyIds = [PROPERTY_ID];
  state.__onEvent = (name, params) => events.push({ name, params });

  const b = createGtagConsentBridge({ windowRef: w, getState: () => state });
  b.start();

  dataLayer.push(makeArgs("consent", "default", { analytics_storage: "denied" }));
  dataLayer.push(makeArgs("consent", "update", { analytics_storage: "granted" }));

  await tick();
  assert.deepEqual(events, []);
});

test("emits a consent-update ping when analytics_storage changes after the init tick (mirrored gtag consent)", async () => {
  const w = makeWindowMock();
  const dataLayer = ensureArraySlot<unknown>(w, "dataLayer");
  const state = createRuntimeState();
  const events: Array<{ name: string; params: unknown }> = [];

  state.propertyIds = [PROPERTY_ID];
  state.__onEvent = (name, params) => events.push({ name, params });

  const b = createGtagConsentBridge({ windowRef: w, getState: () => state });
  b.start();

  dataLayer.push(makeArgs("consent", "default", { analytics_storage: "denied" }));
  await tick();
  dataLayer.push(makeArgs("consent", "update", { analytics_storage: "granted" }));

  assert.deepEqual(events, [{ name: "user_engagement", params: { consent_update: 1 } }]);
});

test("does not emit a consent-update ping before any property is configured", () => {
  const w = makeWindowMock();
  const d8aLayer = ensureArraySlot<unknown>(w, "d8aLayer");
  const events: Array<{ name: string; params: unknown }> = [];

  const consumer = createQueueConsumer({ windowRef: w, dataLayerName: "d8aLayer" });
  consumer.setOnEvent((name, params) => events.push({ name, params }));
  consumer.start();

  d8aLayer.push(makeArgs("consent", "default", { analytics_storage: "denied" }));
  d8aLayer.push(makeArgs("consent", "update", { analytics_storage: "granted" }));

  assert.deepEqual(events, []);
});
