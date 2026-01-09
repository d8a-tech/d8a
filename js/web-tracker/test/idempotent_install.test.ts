import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { PROPERTY_ID, getD8a, makeWindowMock, installSnippetShim } from "./test-utils.ts";

function makeWindow() {
  return makeWindowMock({
    location: { href: "http://example.test/", hostname: "example.test", protocol: "https:" },
    screen: { width: 100, height: 100 },
    document: { title: "T", referrer: "", hidden: false, cookie: "" },
  });
}

test("installD8a is idempotent: double install does not double-patch queue or attach duplicate listeners", async () => {
  const w = makeWindow();
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");

  const r1 = installD8a({ windowRef: w });
  const push1 = dl.push;
  const pagehideCount1 = (w.__listeners.get("pagehide") || []).length;
  const visCount1 = (w.__docListeners.get("visibilitychange") || []).length;

  const r2 = installD8a({ windowRef: w });
  const push2 = dl.push;

  // Same install result object is returned.
  assert.equal(r1, r2);
  // Queue push should not be re-wrapped.
  assert.equal(push1, push2);

  // Ensure we don't process events twice due to nested push wrappers.
  dl.push([
    "config",
    PROPERTY_ID,
    { server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}` },
  ]);
  const before = r1.consumer.getState().events.length;
  dl.push(["event", "purchase", { value: 1 }]);
  const after = r1.consumer.getState().events.length;
  assert.equal(after - before, 1);

  // Listener counts should not increase after the second install.
  // Note: pagehide/visibilitychange are used by both lifecycle flush and engagement timer.
  assert.equal((w.__listeners.get("pagehide") || []).length, pagehideCount1);
  assert.equal((w.__docListeners.get("visibilitychange") || []).length, visCount1);
});

test("installD8a supports multiple instances: different dataLayerName/globalName create independent runtimes", async () => {
  const w = makeWindow();
  const dl2 = ensureArraySlot<unknown>(w, "d8aLayer2");
  const dl3 = ensureArraySlot<unknown>(w, "d8aLayer3");

  // 1) Install instance 1 (base defaults)
  const r1a = installD8a({ windowRef: w }); // default dataLayer: d8aLayer, default global: d8a
  // 2) Install instance 2 (different data layer + different global)
  const r2a = installD8a({ windowRef: w, dataLayerName: "d8aLayer2", globalName: "d8a2" });
  // 3) Install instance 3 (only data layer changes; global stays default)
  const r3a = installD8a({ windowRef: w, dataLayerName: "d8aLayer3" });
  // 4) Install instance 4 (only global changes; data layer stays default)
  const r4a = installD8a({ windowRef: w, globalName: "d8a4" });

  // Re-installing each instance should return the same object for that key.
  const r1b = installD8a({ windowRef: w });
  const r2b = installD8a({ windowRef: w, dataLayerName: "d8aLayer2", globalName: "d8a2" });
  // For r3b/r4b, don't restate the param that stays default.
  const r3b = installD8a({ windowRef: w, dataLayerName: "d8aLayer3" });
  const r4b = installD8a({ windowRef: w, globalName: "d8a4" });
  assert.equal(r1a, r1b);
  assert.equal(r2a, r2b);
  assert.equal(r3a, r3b);
  assert.equal(r4a, r4b);
  assert.notEqual(r1a, r2a);
  assert.notEqual(r1a, r3a);
  assert.notEqual(r1a, r4a);

  // Data layers are independent (push functions are not the same reference across different queues).
  assert.notEqual(ensureArraySlot<unknown>(w, "d8aLayer").push, dl2.push);
  assert.notEqual(ensureArraySlot<unknown>(w, "d8aLayer").push, dl3.push);
  assert.notEqual(dl2.push, dl3.push);

  // Same data layer, different globalName should share the same underlying runtime.
  assert.equal(r1a.consumer, r4a.consumer);
  assert.equal(r1a.dispatcher, r4a.dispatcher);
  assert.notEqual(r1a.consumer, r2a.consumer);
  assert.notEqual(r1a.consumer, r3a.consumer);

  // Each consumer records events from its own queue.
  ensureArraySlot<unknown>(w, "d8aLayer").push(["event", "purchase", { value: 1 }]);
  dl2.push(["event", "purchase", { value: 2 }]);
  dl3.push(["event", "purchase", { value: 3 }]);
  assert.equal(r1a.consumer.getState().events.length, 1);
  assert.equal(r2a.consumer.getState().events.length, 1);
  assert.equal(r3a.consumer.getState().events.length, 1);
  assert.equal(r4a.consumer.getState().events.length, 1);
  assert.equal(r1a.consumer.getState().events[0].params.value, 1);
  assert.equal(r2a.consumer.getState().events[0].params.value, 2);
  assert.equal(r3a.consumer.getState().events[0].params.value, 3);

  // The default global (d8a) must not be overwritten by the 3rd install; it should still push to default d8aLayer.
  installSnippetShim(w, "d8a", "d8aLayer");
  const d8a = getD8a(w);
  d8a("event", "via_global_default", { value: 10 });
  const r1aLast = r1a.consumer.getState().events.at(-1);
  assert.ok(r1aLast);
  assert.equal(r1aLast.params.value, 10);
  const r3aLast = r3a.consumer.getState().events.at(-1);
  assert.ok(r3aLast);
  assert.equal(r3aLast.params.value, 3);

  // The new global (d8a4) should be created and point to the default data layer (same runtime as instance 1).
  installSnippetShim(w, "d8a4", "d8aLayer");
  const d8a4 = getD8a(w, "d8a4");
  d8a4("event", "via_global_4", { value: 11 });
  const r1aLast2 = r1a.consumer.getState().events.at(-1);
  assert.ok(r1aLast2);
  assert.equal(r1aLast2.params.value, 11);
});
