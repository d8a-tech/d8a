import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { PROPERTY_ID, makeWindowMock } from "./test-utils.ts";

function makeWindow() {
  return makeWindowMock({
    location: { hostname: "docs.example.test" },
    document: { title: "T", referrer: "", cookie: "", hidden: false },
    navigator: { language: "en-gb" },
  });
}

test("queue consumer drains existing dataLayer items and patches push", () => {
  const w = makeWindow();
  const d8aLayer = ensureArraySlot<unknown>(w, "d8aLayer");
  d8aLayer.push(["js", new Date("2025-01-01T00:00:00Z")]);
  d8aLayer.push([
    "config",
    PROPERTY_ID,
    { server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c` },
  ]);
  d8aLayer.push(["event", "page_view", { page_location: "https://example.org/" }]);

  // Minimal fetch stub to avoid network.
  w.fetch = async () => ({ status: 0 });

  const { consumer } = installD8a({ windowRef: w });
  const state = consumer.getState();

  assert.equal(state.primaryPropertyId, PROPERTY_ID);
  assert.deepEqual(state.propertyIds, [PROPERTY_ID]);
  // Note: config auto-sends a page_view unless send_page_view is explicitly false.
  assert.equal(state.events.length, 1);
  assert.equal(state.events[0].name, "page_view");

  // New push should be intercepted.
  d8aLayer.push(["event", "purchase", { value: 1 }]);
  assert.equal(state.events.length, 2);
  assert.equal(state.events[1].name, "purchase");
});

test("queue consumer supports custom data layer name", () => {
  const w = makeWindow();
  const myQueue = ensureArraySlot<unknown>(w, "myQueue");
  myQueue.push([
    "config",
    PROPERTY_ID,
    { server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c` },
  ]);
  myQueue.push(["event", "purchase", { value: 1 }]);

  w.fetch = async () => ({ status: 0 });

  const { consumer, dataLayerName } = installD8a({ windowRef: w, dataLayerName: "myQueue" });
  assert.equal(dataLayerName, "myQueue");
  const state = consumer.getState();
  assert.equal(state.events.length, 1);
  assert.equal(state.events[0].name, "purchase");

  myQueue.push(["event", "page_view"]);
  assert.equal(state.events.length, 2);
});
