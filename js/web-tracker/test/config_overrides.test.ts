import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { getD8a, makeWindowMock, PROPERTY_ID } from "./test-utils.ts";

function makeWindow() {
  return makeWindowMock({
    location: { href: "https://example.test/", hostname: "example.test", protocol: "https:" },
    navigator: { language: "browser-lang" },
    document: {
      title: "Browser Title",
      cookie: `_d8a=C1.1.111111111.2222222222`,
      referrer: "https://browser-referrer.example/",
    },
  });
}

test("overrides precedence: event > config > set > browser", () => {
  const w = makeWindow();
  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  // global defaults
  d8a("set", {
    page_location: "https://set.example/",
    campaign_source: "setSource",
    language: "setLang",
    content_group: "setGroup",
  });

  // config overrides set
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    send_page_view: false,
    page_location: "https://cfg.example/",
    campaign_source: "cfgSource",
    language: "cfgLang",
  });

  // event overrides config
  d8a("event", "page_view", {
    page_location: "https://ev.example/",
    campaign_source: "evSource",
  });

  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);

  assert.equal(u.searchParams.get("dl"), "https://ev.example/");
  assert.equal(u.searchParams.get("cs"), "evSource");
  // language: config wins over set (event did not set it)
  assert.equal(u.searchParams.get("ul"), "cfgLang");
  // content_group: set wins over browser (config did not set it)
  assert.equal(u.searchParams.get("ep.content_group"), "setGroup");

  // ensure gtag-style override keys are not duplicated into ep.*
  assert.equal(u.searchParams.get("ep.page_location"), null);
  assert.equal(u.searchParams.get("ep.campaign_source"), null);
});

test("client_id override forces cid (even if cookie exists)", () => {
  const w = makeWindow();
  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    send_page_view: false,
    client_id: "TEST_CLIENT_99999.88888",
  });

  d8a("event", "page_view", {});

  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("cid"), "TEST_CLIENT_99999.88888");
});

test("ignore_referrer=true sends ir=1 but keeps referrer value", () => {
  const w = makeWindow();
  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    send_page_view: false,
    ignore_referrer: true,
  });

  d8a("event", "page_view", {});

  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("ir"), "1");
  // Referrer should still be sent - backend uses ir=1 to decide whether to ignore it
  assert.equal(u.searchParams.get("dr"), "https://browser-referrer.example/");
});
