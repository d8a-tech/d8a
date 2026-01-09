import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { PROPERTY_ID, makeWindowMock, getD8a } from "./test-utils.ts";

function makeWindow() {
  return makeWindowMock({
    location: { href: "https://example.test/", hostname: "example.test", protocol: "https:" },
    navigator: { language: "en-gb" },
  });
}

test("config: send_page_view default triggers automatic page_view", () => {
  const w = makeWindow();
  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  d8a("js", new Date("2025-01-01T00:00:00Z"));
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
  });

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("en"), "page_view");
});

test("config: send_page_view=false disables automatic page_view", () => {
  const w = makeWindow();
  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  d8a("js", new Date("2025-01-01T00:00:00Z"));
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    send_page_view: false,
  });

  assert.equal(w.fetchCalls.length, 0);
});
