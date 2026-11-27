import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { makeWindowMock } from "./test-utils.ts";

function makeWindow() {
  return makeWindowMock({
    location: { href: "http://example.test/", hostname: "example.test", protocol: "https:" },
    screen: { width: 100, height: 100 },
    document: { title: "T", referrer: "", hidden: false, cookie: "" },
  });
}

test("installD8a reports the capabilities it offers to extensions", () => {
  // given
  const w = makeWindow();

  // when
  const { capabilities } = installD8a({ windowRef: w });

  // then
  assert.deepEqual(capabilities, {
    wireParams: true,
    passive: true,
    concurrentFlush: true,
    consentHook: true,
    dispatchTimeWireParams: true,
    configHook: true,
  });
  assert.ok(Object.isFrozen(capabilities));
});

test("a runtime reused by a second install or another global keeps its capabilities", () => {
  // given
  const w = makeWindow();
  const first = installD8a({ windowRef: w });

  // when
  const again = installD8a({ windowRef: w });
  const alias = installD8a({ windowRef: w, globalName: "otherD8a" });

  // then
  assert.equal(again.capabilities, first.capabilities);
  assert.equal(alias.capabilities, first.capabilities);
  assert.equal(alias.consent, first.consent);
});
