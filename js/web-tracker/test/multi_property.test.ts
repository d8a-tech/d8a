import test from "node:test";
import assert from "node:assert/strict";

import { createDispatcher } from "../src/runtime/dispatcher.ts";
import { createCookieJar } from "../src/cookies/cookie_jar.ts";
import { installD8a } from "../src/install.ts";
import { parseD8aSessionCookie } from "../src/cookies/d8a_cookies.ts";
import { createRuntimeState } from "../src/runtime/state.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { getD8a, makeWindowMock, tick, installSnippetShim } from "./test-utils.ts";

const PROPERTY_A = "80e1d6d0-560d-419f-ac2a-fe9281e93386";
const PROPERTY_B = "4f4c6f2b-5b5b-4f41-9f9a-6f4c0b3b0d2a";
const PROPERTY_C = "7b1c2d3e-4f50-6172-8394-a5b6c7d8e9f0";

function installCookieStore(
  w: ReturnType<typeof makeWindowMock>,
  { acceptAll }: { acceptAll: boolean },
) {
  const store = new Map<string, string>();
  Object.defineProperty(w.document, "cookie", {
    configurable: true,
    get: () =>
      Array.from(store.entries())
        .map(([k, v]) => `${k}=${v}`)
        .join("; "),
    set: (v: unknown) => {
      const parts = String(v)
        .split(";")
        .map((s) => s.trim());
      const [nameValue] = parts;
      const idx = nameValue.indexOf("=");
      if (idx < 0) return;
      const name = nameValue.slice(0, idx);
      const value = nameValue.slice(idx + 1);
      if (!acceptAll) {
        // Minimal “browser-like” behavior: only name=value is used; attributes are ignored here.
      }
      store.set(name, value);
    },
  });
}

function makeWindowForInstall() {
  const w = makeWindowMock({
    setTimeout: (fn: () => void) => {
      const id = 1;
      Promise.resolve().then(() => fn());
      return id;
    },
  });
  installCookieStore(w, { acceptAll: true });
  return w;
}

function makeWindow() {
  const w = makeWindowMock();
  installCookieStore(w, { acceptAll: true });
  return w;
}

test("multi-property: default fan-out sends to all configured properties", async () => {
  const w = makeWindow();
  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_A, PROPERTY_B];
  state.propertyConfigs = {
    [PROPERTY_A]: { server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c` },
    [PROPERTY_B]: { server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c` },
  };
  state.consent = { analytics_storage: "granted" };
  state.consentDefault = { analytics_storage: "granted" };
  state.consentUpdate = {};

  const d = createDispatcher({ windowRef: w, getState: () => state });
  d.enqueueEvent("page_view", { a: 1 });

  assert.equal(w.fetchCalls.length, 2);
  assert.match(
    w.fetchCalls[0].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_A}/d/c\\?`),
  );
  assert.match(
    w.fetchCalls[1].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_B}/d/c\\?`),
  );
});

test("multi-property: send_to routes an event to a single property", async () => {
  const w = makeWindow();
  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_A, PROPERTY_B];
  state.propertyConfigs = {
    [PROPERTY_A]: { server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c` },
    [PROPERTY_B]: { server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c` },
  };

  const d = createDispatcher({ windowRef: w, getState: () => state });
  d.enqueueEvent("special_event", { send_to: PROPERTY_B, x: 1 });

  assert.equal(w.fetchCalls.length, 1);
  assert.match(
    w.fetchCalls[0].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_B}/d/c\\?`),
  );
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("ep.send_to"), null);
});

test("multi-property: send_to array routes to a subset of properties", async () => {
  const w = makeWindow();
  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_A, PROPERTY_B, PROPERTY_C];
  state.propertyConfigs = {
    [PROPERTY_A]: { server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c` },
    [PROPERTY_B]: { server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c` },
    [PROPERTY_C]: { server_container_url: `https://tracker.example.test/${PROPERTY_C}/d/c` },
  };

  const d = createDispatcher({ windowRef: w, getState: () => state });
  d.enqueueEvent("special_event", { send_to: [PROPERTY_A, PROPERTY_C], x: 1 });

  assert.equal(w.fetchCalls.length, 2);
  assert.match(
    w.fetchCalls[0].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_A}/d/c\\?`),
  );
  assert.match(
    w.fetchCalls[1].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_C}/d/c\\?`),
  );
});

test("multi-property: session cookie value is written identically for all properties", () => {
  const w = makeWindow();
  const jar = createCookieJar({ windowRef: w });

  const state = createRuntimeState();
  state.propertyIds = [PROPERTY_A, PROPERTY_B];
  state.propertyConfigs = {
    [PROPERTY_A]: { server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c` },
    [PROPERTY_B]: { server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c` },
  };
  state.consent = { analytics_storage: "granted" };

  const d = createDispatcher({ windowRef: w, getState: () => state });
  d.enqueueEvent("page_view", {});

  const a = jar.get(`_d8a_${PROPERTY_A}`);
  const b = jar.get(`_d8a_${PROPERTY_B}`);
  assert.ok(a, `expected _d8a_${PROPERTY_A} to exist`);
  assert.ok(b, `expected _d8a_${PROPERTY_B} to exist`);
  assert.equal(a, b, "expected session cookie values to be identical across properties");
});

test("multi-property: two independent trackers with different globals and datalayers", async () => {
  const w = makeWindowForInstall();
  // Verify independence of data layers
  const dl1 = ensureArraySlot<unknown>(w, "d8aLayer");
  const dl2 = ensureArraySlot<unknown>(w, "d8aLayer2");

  // Install instance 1 (default: global=d8a, queue=d8aLayer)
  const r1 = installD8a({ windowRef: w });
  // Simulate snippet pushing array-like arguments.
  installSnippetShim(w, "d8a", "d8aLayer");

  // Install instance 2 (global=d8a2, queue=d8aLayer2)
  const r2 = installD8a({ windowRef: w, dataLayerName: "d8aLayer2", globalName: "d8a2" });
  installSnippetShim(w, "d8a2", "d8aLayer2");

  // Verify they are independent
  assert.notEqual(r1.consumer, r2.consumer);
  assert.notEqual(r1.dispatcher, r2.dispatcher);
  assert.notEqual(dl1.push, dl2.push);

  // Configure property A on instance 1
  const d8a1 = getD8a(w);
  d8a1("js", new Date("2025-01-01T00:00:00Z"));
  d8a1("config", PROPERTY_A, {
    server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c`,
    cookie_prefix: "prop1_",
    send_page_view: false,
  });

  // Configure property B on instance 2
  const d8a2 = getD8a(w, "d8a2");
  d8a2("js", new Date("2025-01-01T00:00:00Z"));
  d8a2("config", PROPERTY_B, {
    server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c`,
    cookie_prefix: "prop2_",
    send_page_view: false,
  });

  // Send events to each tracker
  d8a1("event", "page_view", {});
  d8a2("event", "page_view", {});

  await tick();

  // Verify events are sent to correct endpoints
  assert.equal(w.fetchCalls.length, 2);
  assert.match(
    w.fetchCalls[0].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_A}/d/c\\?`),
  );
  assert.match(
    w.fetchCalls[1].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_B}/d/c\\?`),
  );
});

test("multi-property: two independent trackers have isolated client ID cookies", async () => {
  const w = makeWindowForInstall();

  const jar = createCookieJar({ windowRef: w });

  // Install instance 1
  installD8a({ windowRef: w });
  installSnippetShim(w, "d8a", "d8aLayer");

  // Install instance 2
  installD8a({ windowRef: w, dataLayerName: "d8aLayer2", globalName: "d8a2" });
  installSnippetShim(w, "d8a2", "d8aLayer2");

  // Configure with different cookie prefixes
  const d8a1 = getD8a(w);
  d8a1("js", new Date("2025-01-01T00:00:00Z"));
  d8a1("config", PROPERTY_A, {
    server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c`,
    cookie_prefix: "prop1_",
    send_page_view: false,
  });

  const d8a2 = getD8a(w, "d8a2");
  d8a2("js", new Date("2025-01-01T00:00:00Z"));
  d8a2("config", PROPERTY_B, {
    server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c`,
    cookie_prefix: "prop2_",
    send_page_view: false,
  });

  // Send events to trigger cookie creation
  d8a1("event", "page_view", {});
  d8a2("event", "page_view", {});

  await tick();

  // Verify client ID cookies are separate
  const clientId1 = jar.get("prop1_d8a");
  const clientId2 = jar.get("prop2_d8a");

  assert.ok(clientId1, "expected prop1_d8a client ID cookie to exist");
  assert.ok(clientId2, "expected prop2_d8a client ID cookie to exist");
  assert.notEqual(clientId1, clientId2, "expected client ID cookies to be different");
});

test("multi-property: two independent trackers have isolated session cookies", async () => {
  const w = makeWindowForInstall();

  const jar = createCookieJar({ windowRef: w });

  // Install instance 1
  installD8a({ windowRef: w });
  installSnippetShim(w, "d8a", "d8aLayer");

  // Install instance 2
  installD8a({ windowRef: w, dataLayerName: "d8aLayer2", globalName: "d8a2" });
  installSnippetShim(w, "d8a2", "d8aLayer2");

  // Configure with different cookie prefixes
  const d8a1 = getD8a(w);
  d8a1("js", new Date("2025-01-01T00:00:00Z"));
  d8a1("config", PROPERTY_A, {
    server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c`,
    cookie_prefix: "prop1_",
    send_page_view: false,
  });

  const d8a2 = getD8a(w, "d8a2");
  d8a2("js", new Date("2025-01-01T00:00:00Z"));
  d8a2("config", PROPERTY_B, {
    server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c`,
    cookie_prefix: "prop2_",
    send_page_view: false,
  });

  // Send events to trigger session cookie creation
  d8a1("event", "page_view", {});
  d8a2("event", "page_view", {});

  await tick();

  // Verify session cookies are separate and have different values
  const session1 = jar.get(`prop1_d8a_${PROPERTY_A}`);
  const session2 = jar.get(`prop2_d8a_${PROPERTY_B}`);

  assert.ok(session1, `expected prop1_d8a_${PROPERTY_A} session cookie to exist`);
  assert.ok(session2, `expected prop2_d8a_${PROPERTY_B} session cookie to exist`);
  assert.notEqual(session1, session2, "expected session cookies to have different values");
});

test("multi-property: independent trackers have isolated hit counters in session cookies", async () => {
  const w = makeWindowForInstall();

  const jar = createCookieJar({ windowRef: w });

  // Install instance 1
  installD8a({ windowRef: w });
  installSnippetShim(w, "d8a", "d8aLayer");

  // Install instance 2
  installD8a({ windowRef: w, dataLayerName: "d8aLayer2", globalName: "d8a2" });
  installSnippetShim(w, "d8a2", "d8aLayer2");

  // Configure with different cookie prefixes
  const d8a1 = getD8a(w);
  d8a1("js", new Date("2025-01-01T00:00:00Z"));
  d8a1("config", PROPERTY_A, {
    server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c`,
    cookie_prefix: "prop1_",
    send_page_view: false,
  });

  const d8a2 = getD8a(w, "d8a2");
  d8a2("js", new Date("2025-01-01T00:00:00Z"));
  d8a2("config", PROPERTY_B, {
    server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c`,
    cookie_prefix: "prop2_",
    send_page_view: false,
  });

  await tick();

  // Send multiple events to tracker 1
  d8a1("event", "page_view", {});
  await tick();
  d8a1("event", "custom_event_1", {});
  await tick();
  d8a1("event", "custom_event_2", {});
  await tick();

  // Send only one event to tracker 2
  d8a2("event", "page_view", {});
  await tick();

  // Parse session cookies to check hit counter (j token)
  const session1Value = jar.get(`prop1_d8a_${PROPERTY_A}`);
  const session2Value = jar.get(`prop2_d8a_${PROPERTY_B}`);

  assert.ok(session1Value, `expected prop1_d8a_${PROPERTY_A} session cookie to exist`);
  assert.ok(session2Value, `expected prop2_d8a_${PROPERTY_B} session cookie to exist`);

  const session1 = parseD8aSessionCookie(session1Value);
  const session2 = parseD8aSessionCookie(session2Value);
  assert.ok(session1, "expected tracker 1 session cookie to parse");
  assert.ok(session2, "expected tracker 2 session cookie to parse");

  // Find the 'j' token (hit counter) in each session
  const j1 = session1.tokens?.find((t) => t.key === "j")?.val;
  const j2 = session2.tokens?.find((t) => t.key === "j")?.val;

  assert.ok(j1 != null, "expected tracker 1 session cookie to have hit counter (j token)");
  assert.ok(j2 != null, "expected tracker 2 session cookie to have hit counter (j token)");

  // Tracker 1 received 3 events, tracker 2 received 1 event
  // So tracker 1's hit counter should be higher
  assert.ok(
    Number(j1) > Number(j2),
    `expected tracker 1 hit counter (${j1}) to be greater than tracker 2 hit counter (${j2})`,
  );
});

test("multi-property: events sent to one independent tracker do not affect the other", async () => {
  const w = makeWindowForInstall();

  // Install instance 1
  installD8a({ windowRef: w });
  installSnippetShim(w, "d8a", "d8aLayer");

  // Install instance 2
  installD8a({ windowRef: w, dataLayerName: "d8aLayer2", globalName: "d8a2" });
  installSnippetShim(w, "d8a2", "d8aLayer2");

  // Configure both
  const d8a1 = getD8a(w);
  d8a1("js", new Date("2025-01-01T00:00:00Z"));
  d8a1("config", PROPERTY_A, {
    server_container_url: `https://tracker.example.test/${PROPERTY_A}/d/c`,
    cookie_prefix: "prop1_",
    send_page_view: false,
  });

  const d8a2 = getD8a(w, "d8a2");
  d8a2("js", new Date("2025-01-01T00:00:00Z"));
  d8a2("config", PROPERTY_B, {
    server_container_url: `https://tracker.example.test/${PROPERTY_B}/d/c`,
    cookie_prefix: "prop2_",
    send_page_view: false,
  });

  await tick();
  // Clear fetch calls after initial config processing
  w.fetchCalls = [];

  // Send event only to instance 1
  d8a1("event", "custom_event", { test_param: "value1" });

  await tick();

  // Verify only instance 1 received the event
  assert.equal(w.fetchCalls.length, 1);
  assert.match(
    w.fetchCalls[0].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_A}/d/c\\?`),
  );
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("ep.test_param"), "value1");

  // Clear and send event only to instance 2
  w.fetchCalls = [];
  d8a2("event", "custom_event", { test_param: "value2" });

  await tick();

  // Verify only instance 2 received the event
  assert.equal(w.fetchCalls.length, 1);
  assert.match(
    w.fetchCalls[0].url,
    new RegExp(`^https://tracker\\.example\\.test/${PROPERTY_B}/d/c\\?`),
  );
  const u2 = new URL(w.fetchCalls[0].url);
  assert.equal(u2.searchParams.get("ep.test_param"), "value2");
});
