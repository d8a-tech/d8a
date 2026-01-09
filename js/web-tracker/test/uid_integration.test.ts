import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { PROPERTY_ID, makeWindowMock, getD8a } from "./test-utils.ts";

function makeWindow() {
  const w = makeWindowMock({
    location: { href: "https://example.test/", hostname: "example.test", protocol: "https:" },
  });
  ensureArraySlot<unknown>(w, "dataLayer");
  return w;
}

test("uid: user_id can be set globally and maps to uid", () => {
  const w = makeWindow();

  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  d8a("js", new Date("2025-01-01T00:00:00Z"));
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c`,
    send_page_view: false,
  });

  d8a("set", { user_id: "user-1" });
  d8a("event", "page_view", {});

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("uid"), "user-1");
});

test("uid: event user_id overrides global user_id for that request", () => {
  const w = makeWindow();

  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  d8a("js", new Date("2025-01-01T00:00:00Z"));
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c`,
    send_page_view: false,
  });

  d8a("set", { user_id: "user-1" });
  d8a("event", "page_view", { user_id: "user-2" });

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("uid"), "user-2");
});
