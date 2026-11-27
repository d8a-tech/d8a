import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { makeWindowMock } from "./test-utils.ts";

const A = "11111111-1111-4111-8111-111111111111";
const B = "22222222-2222-4222-8222-222222222222";

function makeWindow() {
  return makeWindowMock({
    location: { href: "http://example.test/", hostname: "example.test", protocol: "https:" },
    screen: { width: 100, height: 100 },
    document: { title: "T", referrer: "", hidden: false, cookie: "" },
  });
}

const config = (pid: string) => ["config", pid, { send_page_view: false }];

test("config hook replays properties configured before subscribing, once each", () => {
  // given: the inline snippet configured a property before the extension subscribes
  const w = makeWindow();
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  dl.push(config(A));
  const { config: hook } = installD8a({ windowRef: w });
  const seen: string[] = [];

  // when
  hook.onPropertyConfigured((pid) => seen.push(pid));

  // then
  assert.deepEqual(seen, [A]);
});

test("config hook reports later config calls, repeats and new properties included", () => {
  // given
  const w = makeWindow();
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  const { config: hook } = installD8a({ windowRef: w });
  const seen: string[] = [];
  hook.onPropertyConfigured((pid) => seen.push(pid));

  // when
  dl.push(config(A));
  dl.push(config(A));
  dl.push(config(B));

  // then
  assert.deepEqual(seen, [A, A, B]);
});

test("config hook listeners can unsubscribe and never break configuration", () => {
  // given
  const w = makeWindow();
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  const { config: hook, consumer } = installD8a({ windowRef: w });
  const seen: string[] = [];
  hook.onPropertyConfigured(() => {
    throw new Error("extension bug");
  });
  const unsubscribe = hook.onPropertyConfigured((pid) => seen.push(pid));

  // when
  dl.push(config(A));
  unsubscribe();
  dl.push(config(B));

  // then
  assert.deepEqual(seen, [A]);
  assert.deepEqual((consumer.getState() as { propertyIds: string[] }).propertyIds, [A, B]);
});

test("an alias install shares the config hook", () => {
  // given
  const w = makeWindow();
  const first = installD8a({ windowRef: w });

  // when
  const alias = installD8a({ windowRef: w, globalName: "otherD8a" });

  // then
  assert.equal(alias.config, first.config);
});
