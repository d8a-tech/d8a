import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { makeWindowMock } from "./test-utils.ts";

function makeWindow() {
  const w = makeWindowMock({
    location: { href: "http://example.test/", hostname: "example.test", protocol: "https:" },
    screen: { width: 100, height: 100 },
    document: { title: "T", referrer: "", hidden: false, cookie: "" },
  }) as ReturnType<typeof makeWindowMock> & { dataLayer: unknown[] };
  w.dataLayer = [];
  return w;
}

test("consent hook reports the effective analytics_storage from d8a consent commands", () => {
  // given
  const w = makeWindow();
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  const { consent } = installD8a({ windowRef: w });
  const before = consent.analyticsStorage();

  // when
  dl.push(["consent", "default", { analytics_storage: "denied" }]);
  const afterDefault = consent.analyticsStorage();
  dl.push(["consent", "update", { analytics_storage: "granted" }]);

  // then
  assert.equal(before, undefined);
  assert.equal(afterDefault, "denied");
  assert.equal(consent.analyticsStorage(), "granted");
});

test("consent hook prefers mirrored gtag consent", () => {
  // given
  const w = makeWindow();
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  const { consent } = installD8a({ windowRef: w });

  // when
  dl.push(["consent", "default", { analytics_storage: "granted" }]);
  w.dataLayer.push(["consent", "default", { analytics_storage: "denied" }]);

  // then
  assert.equal(consent.analyticsStorage(), "denied");
});

test("consent hook notifies every change once, including same-tick default and update", () => {
  // given
  const w = makeWindow();
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  const { consent } = installD8a({ windowRef: w });
  const seen: unknown[] = [];
  consent.onAnalyticsStorageChange((value) => seen.push(value));

  // when
  dl.push(["consent", "default", { analytics_storage: "denied" }]);
  dl.push(["consent", "update", { analytics_storage: "granted" }]);
  dl.push(["consent", "update", { analytics_storage: "granted" }]);
  dl.push(["consent", "update", { analytics_storage: "denied" }]);

  // then
  assert.deepEqual(seen, ["denied", "granted", "denied"]);
});

test("consent hook listeners can unsubscribe and never break consent handling", () => {
  // given
  const w = makeWindow();
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  const { consent } = installD8a({ windowRef: w });
  const seen: unknown[] = [];
  consent.onAnalyticsStorageChange(() => {
    throw new Error("extension bug");
  });
  const unsubscribe = consent.onAnalyticsStorageChange((value) => seen.push(value));

  // when
  dl.push(["consent", "default", { analytics_storage: "denied" }]);
  unsubscribe();
  dl.push(["consent", "update", { analytics_storage: "granted" }]);

  // then
  assert.deepEqual(seen, ["denied"]);
  assert.equal(consent.analyticsStorage(), "granted");
});
