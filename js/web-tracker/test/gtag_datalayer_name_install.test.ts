import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { makeWindowMock, ensureArraySlot } from "./test-utils.ts";

function makeWindow() {
  return makeWindowMock({
    location: { href: "http://example.test/", hostname: "example.test", protocol: "https:" },
    navigator: { language: "en-gb" },
    document: {
      title: "T",
    },
  });
}

test("installD8a: gtag consent bridge can be configured via window.d8aGtagDataLayerName", () => {
  const w = makeWindow();
  const myGtagLayer = ensureArraySlot<unknown>(w, "myGtagLayer");
  (w as any).d8aGtagDataLayerName = "myGtagLayer";

  // Pre-seed gtag consent update into the custom queue.
  myGtagLayer.push(["consent", "update", { analytics_storage: "granted" }]);

  const r = installD8a({ windowRef: w });
  const s = r.consumer.getState();

  assert.equal(s.consentGtag.analytics_storage, "granted");
  // Should not create/patch the default dataLayer as a side-effect when configured.
  assert.equal(w.dataLayer, undefined);
});

test("installD8a: gtag consent bridge can be configured via installD8a({ gtagDataLayerName })", () => {
  const w = makeWindow();
  const myGtagLayer = ensureArraySlot<unknown>(w, "myGtagLayer");

  myGtagLayer.push(["consent", "update", { analytics_storage: "granted" }]);

  const r = installD8a({ windowRef: w, gtagDataLayerName: "myGtagLayer" });
  const s = r.consumer.getState();

  assert.equal(s.consentGtag.analytics_storage, "granted");
  assert.equal(w.dataLayer, undefined);
});
