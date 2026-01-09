import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { PROPERTY_ID, makeWindowMock, getD8a } from "./test-utils.ts";

function makeWindow() {
  const w = makeWindowMock({
    location: { href: "https://example.test/", hostname: "example.test", protocol: "https:" },
  });
  // gtag snippet uses `window.dataLayer`.
  ensureArraySlot<unknown>(w, "dataLayer");
  return w;
}

test("consent via d8a() affects outgoing gcs/gcd", async () => {
  const w = makeWindow();
  const { dispatcher } = installD8a({ windowRef: w });

  const d8a = getD8a(w);

  d8a("js", new Date("2025-01-01T00:00:00Z"));
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    send_page_view: false,
  });

  d8a("consent", "default", {
    ad_storage: "denied",
    analytics_storage: "denied",
    ad_user_data: "denied",
    ad_personalization: "denied",
  });
  d8a("consent", "update", {
    ad_storage: "granted",
    analytics_storage: "granted",
    ad_user_data: "granted",
    ad_personalization: "granted",
  });

  d8a("event", "page_view", {});

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("gcs"), "G111");
  assert.equal(u.searchParams.get("gcd"), "13r3r3r2r5l1");

  // quiet unused warning / make intent explicit
  assert.ok(dispatcher);
});

test("consent via gtag()/dataLayer is preferred over d8a consent", () => {
  const w = makeWindow();

  // gtag snippet behavior: push arguments into `window.dataLayer`
  const dataLayer = ensureArraySlot<unknown>(w, "dataLayer");
  function gtag(...args: unknown[]) {
    dataLayer.push(args);
  }

  // gtag consent: default denied -> update granted
  gtag("consent", "default", {
    ad_storage: "denied",
    analytics_storage: "denied",
    ad_user_data: "denied",
    ad_personalization: "denied",
  });
  gtag("consent", "update", {
    ad_storage: "granted",
    analytics_storage: "granted",
    ad_user_data: "granted",
    ad_personalization: "granted",
  });

  installD8a({ windowRef: w });
  const d8a = getD8a(w);
  d8a("js", new Date("2025-01-01T00:00:00Z"));
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    send_page_view: false,
  });

  // d8a consent denied (should be ignored because gtag consent is available)
  d8a("consent", "default", { ad_storage: "denied", analytics_storage: "denied" });
  d8a("consent", "update", { ad_storage: "denied", analytics_storage: "denied" });

  d8a("event", "page_view", {});

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("gcd"), "13r3r3r2r5l1");
});
