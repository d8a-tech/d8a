import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { getD8a, makeWindowMock, PROPERTY_ID } from "./test-utils.ts";

test("browser context: all browser properties are translated to tracking request parameters", () => {
  const w = makeWindowMock({
    location: {
      href: "https://example.test/page?query=1",
      hostname: "example.test",
      protocol: "https:",
    },
    navigator: { language: "en-GB" }, // uppercase language
    screen: { width: 1920, height: 1080 },
    document: {
      title: "Test Page Title",
      cookie: `_d8a=C1.1.111111111.2222222222`,
      referrer: "https://referrer.example.test/",
    },
  });
  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/${PROPERTY_ID}/d/c`,
  });

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);

  // dl (document location) -> "dl"
  assert.equal(u.searchParams.get("dl"), "https://example.test/page?query=1");
  // dt (document title) -> "dt"
  assert.equal(u.searchParams.get("dt"), "Test Page Title");
  // dr (document referrer) -> "dr"
  assert.equal(u.searchParams.get("dr"), "https://referrer.example.test/");
  // ul (user language) -> "ul" (lowercase)
  assert.equal(u.searchParams.get("ul"), "en-gb");
  // sr (screen resolution) -> "sr"
  assert.equal(u.searchParams.get("sr"), "1920x1080");
});
