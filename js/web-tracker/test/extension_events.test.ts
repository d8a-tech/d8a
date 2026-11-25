import test from "node:test";
import assert from "node:assert/strict";

import { createDispatcher } from "../src/runtime/dispatcher.ts";
import { createRuntimeState } from "../src/runtime/state.ts";
import { PROPERTY_ID, TRACKER_DOMAIN, makeWindowMock } from "./test-utils.ts";

function setup({ fetchNeverResolves = false } = {}) {
  const w = makeWindowMock({ setTimeout: () => 1 });
  if (fetchNeverResolves) {
    w.fetch = (url: string, opts?: unknown) => {
      w.fetchCalls.push({ url, opts });
      return new Promise(() => {});
    };
  }
  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_ID];
  state.propertyConfigs = {
    [PROPERTY_ID]: { server_container_url: `${TRACKER_DOMAIN}/${PROPERTY_ID}/d/c` },
  };
  const d = createDispatcher({ windowRef: w, getState: () => state });
  return { w, d };
}

function sentParams(call: { url: string; opts?: unknown }) {
  const body = (call.opts as { body?: string } | undefined)?.body;
  return body != null ? new URLSearchParams(body) : new URL(call.url).searchParams;
}

test("wireParams are sent without the ep. prefix", async () => {
  // given
  const { w, d } = setup();

  // when
  d.enqueueEvent("ext_event", { plain: "x" }, { wireParams: { _xa: "1", _xb: "two" } });
  await d.flushNow();

  // then
  const p = sentParams(w.fetchCalls[0]);
  assert.equal(p.get("en"), "ext_event");
  assert.equal(p.get("_xa"), "1");
  assert.equal(p.get("_xb"), "two");
  assert.equal(p.get("ep.plain"), "x");
  assert.equal(p.get("ep._xa"), null);
});

test("a hit with a long query string is sent in the POST body", async () => {
  // given
  const { w, d } = setup();
  const big = JSON.stringify({ data: "y".repeat(3000) });

  // when
  d.enqueueEvent("short_event", {});
  d.enqueueEvent("long_event", {}, { wireParams: { _xj: big } });
  await d.flushNow();

  // then
  const [shortCall, longCall] = w.fetchCalls;
  assert.equal((shortCall.opts as { body?: string }).body, undefined);
  assert.equal(new URL(shortCall.url).searchParams.get("en"), "short_event");
  assert.equal(longCall.url, `${TRACKER_DOMAIN}/${PROPERTY_ID}/d/c`);
  const p = sentParams(longCall);
  assert.equal(p.get("en"), "long_event");
  assert.equal(p.get("_xj"), big);
  assert.equal(p.get("tid"), PROPERTY_ID);
});

test("a passive event leaves the engagement time to the next event", async () => {
  // given
  const realNow = Date.now;
  let now = 1_000;
  Date.now = () => now;
  try {
    const { w, d } = setup();
    d.enqueueEvent("page_view", {});
    await d.flushNow();
    now += 4_000;

    // when
    d.enqueueEvent("ext_event", {}, { passive: true });
    d.enqueueEvent("user_engagement", {});
    await d.flushNow();

    // then
    assert.equal(sentParams(w.fetchCalls[1]).get("_et"), "0");
    assert.equal(sentParams(w.fetchCalls[2]).get("_et"), "4000");
  } finally {
    Date.now = realNow;
  }
});

test("a concurrent flush starts every request before any response", () => {
  // given
  const { w, d } = setup({ fetchNeverResolves: true });
  d.enqueueEvent("a", {});
  d.enqueueEvent("b", {});
  d.enqueueEvent("c", {});

  // when
  void d.flushNow({ concurrent: true });

  // then
  assert.deepEqual(
    w.fetchCalls.map((c) => new URL(c.url).searchParams.get("en")),
    ["a", "b", "c"],
  );
});

test("a regular flush waits for each request before sending the next", () => {
  // given
  const { w, d } = setup({ fetchNeverResolves: true });
  d.enqueueEvent("a", {});
  d.enqueueEvent("b", {});

  // when
  void d.flushNow();

  // then
  assert.equal(w.fetchCalls.length, 1);
});
