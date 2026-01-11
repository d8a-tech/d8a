import test from "node:test";
import assert from "node:assert/strict";

import { createLinker } from "../src/runtime/linker.ts";
import { createRuntimeState } from "../src/runtime/state.ts";
import { createCookieJar } from "../src/cookies/cookie_jar.ts";
import { ensureClientId, ensureSession } from "../src/cookies/identity.ts";
import { PROPERTY_ID } from "./test-utils.ts";

const PID1 = PROPERTY_ID;
const PID2 = "f1b6b8a2-07fa-4e2b-8a4e-30b0dcb8a9a4";
const PID3 = "c7b0d9f0-8d2b-4e4b-8b9f-7c6d9a4b0d1e";

type Listener = (...args: unknown[]) => void;

function makeDocumentCookieStore() {
  const store = new Map<string, string>();
  return {
    __store: store,
    get cookie(): string {
      return Array.from(store.entries())
        .map(([k, v]) => `${k}=${v}`)
        .join("; ");
    },
    set cookie(v: string) {
      const parts = String(v)
        .split(";")
        .map((s) => s.trim());
      const nameValue = parts[0] || "";
      const idx = nameValue.indexOf("=");
      if (idx < 0) return;
      const name = nameValue.slice(0, idx);
      const value = nameValue.slice(idx + 1);

      // expires delete
      const expiresAttr = parts.find((a) => a.toLowerCase().startsWith("expires="));
      if (expiresAttr && expiresAttr.toLowerCase().includes("thu, 01 jan 1970")) {
        store.delete(name);
        return;
      }
      // Max-Age=0 deletes immediately
      const maxAgeAttr = parts.find((a) => a.toLowerCase().startsWith("max-age="));
      if (maxAgeAttr) {
        const vRaw = maxAgeAttr.split("=").slice(1).join("=").trim();
        if (String(Number(vRaw)) === vRaw && Number(vRaw) <= 0) {
          store.delete(name);
          return;
        }
      }

      store.set(name, value);
    },
  };
}

function makeWindowWithCookieStore({
  href,
  hostname,
  protocol = "https:",
}: {
  href: string;
  hostname: string;
  protocol?: string;
}) {
  const docListeners = new Map<string, Listener[]>();
  const listeners = new Map<string, Listener[]>();
  const cookieStore = makeDocumentCookieStore();

  function push(map: Map<string, Listener[]>, t: string, fn: Listener) {
    const arr = map.get(t);
    if (arr) arr.push(fn);
    else map.set(t, [fn]);
  }
  function remove(map: Map<string, Listener[]>, t: string, fn: Listener) {
    const arr = map.get(t);
    if (!arr) return;
    map.set(
      t,
      arr.filter((x) => x !== fn),
    );
  }

  const document: any = {
    title: "T",
    hidden: false,
    addEventListener: (t: string, fn: Listener) => push(docListeners, t, fn),
    removeEventListener: (t: string, fn: Listener) => remove(docListeners, t, fn),
  };
  Object.defineProperty(document, "cookie", {
    enumerable: true,
    get: () => cookieStore.cookie,
    set: (v: string) => {
      cookieStore.cookie = v;
    },
  });

  const w: any = {
    location: { href, hostname, protocol },
    navigator: { language: "en-gb", userAgent: "UA-test", sendBeacon: null },
    document,
    history: {
      replaceState: (_data: unknown, _unused: string, url?: string) => {
        if (url) w.location.href = String(url);
      },
    },
    addEventListener: (t: string, fn: Listener) => push(listeners, t, fn),
    removeEventListener: (t: string, fn: Listener) => remove(listeners, t, fn),
    setTimeout: (fn: Listener) => {
      fn();
      return 1;
    },
    clearTimeout: () => {},
    __listeners: listeners,
    __docListeners: docListeners,
  };

  return w;
}

function extractDl(url: string) {
  const idx = url.indexOf("_dl=");
  if (idx < 0) return null;
  const rest = url.slice(idx + "_dl=".length);
  const end = rest.indexOf("&");
  const raw = end < 0 ? rest : rest.slice(0, end);
  return decodeURIComponent(raw);
}

test("linker: encode/decode round-trip (ttl + fingerprint)", () => {
  const w = makeWindowWithCookieStore({
    href: "https://a.example.test/",
    hostname: "a.example.test",
  });
  const state = createRuntimeState();
  const linker = createLinker({ windowRef: w, getState: () => state });

  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const encoded = (linker.__internal as any).encodeDl({
      cookies: { _d8a: "C1.1.123.456", [`_d8a_${PID1}`]: "S1.1.s1$o1$g0$t1$j0$dabc" },
      ts: Date.now(),
      windowRef: w,
    });
    const decoded = (linker.__internal as any).decodeDl({ value: encoded, windowRef: w });
    assert.ok(decoded);
    assert.equal(decoded.ts, 1700000000000);
    assert.equal(decoded.cookies._d8a, "C1.1.123.456");
    assert.equal(decoded.cookies[`_d8a_${PID1}`], "S1.1.s1$o1$g0$t1$j0$dabc");
  } finally {
    Date.now = realNow;
  }
});

test("linker: decorates URLs with _dl carrying multiple property cookies (when present)", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-a.example.test/",
    hostname: "site-a.example.test",
  });
  const jar = createCookieJar({ windowRef: w });

  // Create valid d8a cookies for three properties and a shared client cookie.
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
  });
  ensureSession({
    jar,
    propertyId: PID1,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    sessionTimeoutMs: 30 * 60 * 1000,
  });
  ensureSession({
    jar,
    propertyId: PID2,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    sessionTimeoutMs: 30 * 60 * 1000,
  });
  ensureSession({
    jar,
    propertyId: PID3,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    sessionTimeoutMs: 30 * 60 * 1000,
  });
  assert.ok(jar.get(`_d8a_${PID1}`));
  assert.ok(jar.get(`_d8a_${PID2}`));
  assert.ok(jar.get(`_d8a_${PID3}`));

  const state = createRuntimeState();
  state.linker = { domains: ["dest.example.test"] };
  state.propertyIds = [PID1, PID2, PID3];
  state.propertyConfigs[PID1] = {};
  state.propertyConfigs[PID2] = {};
  state.propertyConfigs[PID3] = {};

  const linker = createLinker({ windowRef: w, getState: () => state });

  const decorated = (linker.__internal as any).decorateUrlIfNeeded(
    "https://dest.example.test/path",
  );
  const dl = extractDl(decorated);
  assert.ok(dl, "expected _dl param to be present");

  const decoded = (linker.__internal as any).decodeDl({ value: dl, windowRef: w });
  assert.ok(decoded);
  // Must include at least the cookies relevant to configured properties.
  assert.ok(
    Object.keys(decoded.cookies).some((k) => k.includes("_d8a")),
    "expected d8a cookies",
  );
  assert.ok(decoded.cookies[`_d8a_${PID1}`]);
  assert.ok(decoded.cookies[`_d8a_${PID2}`]);
  assert.ok(decoded.cookies[`_d8a_${PID3}`]);
});

test("linker: accepts incoming _dl, applies cookies, and strips param from URL", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/?x=1",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });

  const state = createRuntimeState();
  state.linker = { domains: ["site-a.example.test"] }; // accept_incoming defaults true when domains present
  state.propertyIds = [PID1, PID2, PID3];
  state.propertyConfigs[PID1] = {};
  state.propertyConfigs[PID2] = {};
  state.propertyConfigs[PID3] = {};

  const linker = createLinker({ windowRef: w, getState: () => state });

  // Build a valid outgoing payload from a synthetic cookie list.
  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const dl = (linker.__internal as any).encodeDl({
      cookies: {
        _d8a: "C1.1.999.888",
        [`_d8a_${PID1}`]: "S1.1.s1$o1$g0$t1$j0$dabc",
        [`_d8a_${PID2}`]: "S1.1.s2$o1$g0$t2$j0$ddef",
        [`_d8a_${PID3}`]: "S1.1.s3$o1$g0$t3$j0$dghi",
      },
      ts: Date.now(),
      windowRef: w,
    });
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}&x=1`;

    linker.start();

    // Applied cookies
    assert.ok(jar.get("_d8a"), "client cookie should be written");
    assert.ok(jar.get(`_d8a_${PID1}`), "session cookie should be written");
    assert.ok(jar.get(`_d8a_${PID2}`), "session cookie should be written");
    assert.ok(jar.get(`_d8a_${PID3}`), "session cookie should be written");

    // Stripped from URL
    const href = w.location.href;
    assert.ok(href, "expected location.href to be set");
    assert.ok(!href.includes("_dl="));
  } finally {
    Date.now = realNow;
  }
});

test("linker: honors cookie_update=false for existing cookies (no overwrite)", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });

  // Existing cookie on destination.
  jar.set("_d8a", "C1.1.111.222", { domain: "auto" });

  const state = createRuntimeState();
  state.linker = { domains: ["site-a.example.test"] };
  state.set = { cookie_update: false }; // global default cookie_update=false

  const linker = createLinker({ windowRef: w, getState: () => state });

  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const dl = (linker.__internal as any).encodeDl({
      cookies: { _d8a: "C1.1.999.888" },
      ts: Date.now(),
      windowRef: w,
    });
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}`;
    linker.start();

    // Should keep existing cookie value.
    assert.equal(jar.get("_d8a"), "C1.1.111.222");
  } finally {
    Date.now = realNow;
  }
});

test("linker: cookie_update=true overwrites existing cookies on incoming _dl", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });

  // Existing cookies on destination (different values).
  jar.set("_d8a", "C1.1.111.222", { domain: "auto" });
  jar.set(`_d8a_${PID1}`, "S1.1.s10$o1$g0$t10$j0$dold", { domain: "auto" });

  const state = createRuntimeState();
  state.linker = { domains: ["site-a.example.test"] };
  state.propertyIds = [PID1];
  state.propertyConfigs[PID1] = {};

  const linker = createLinker({ windowRef: w, getState: () => state });

  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const dl = (linker.__internal as any).encodeDl({
      cookies: { _d8a: "C1.1.999.888", [`_d8a_${PID1}`]: "S1.1.s1$o1$g0$t1$j0$dnew" },
      ts: Date.now(),
      windowRef: w,
    });
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}`;
    linker.start();

    assert.equal(jar.get("_d8a"), "C1.1.999.888");
    assert.equal(jar.get(`_d8a_${PID1}`), "S1.1.s1$o1$g0$t1$j0$dnew");
  } finally {
    Date.now = realNow;
  }
});

test("linker: encodes multiple client cookies when cookie_prefix differs across properties", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-a.example.test/",
    hostname: "site-a.example.test",
  });
  const jar = createCookieJar({ windowRef: w });

  // Simulate two independent trackers (different cookie_prefix).
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookiePrefix: "P1_",
  });
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookiePrefix: "P2_",
  });

  const state = createRuntimeState();
  state.linker = { domains: ["dest.example.test"] };
  state.propertyIds = [PID1, PID2];
  state.propertyConfigs[PID1] = { cookie_prefix: "P1_" };
  state.propertyConfigs[PID2] = { cookie_prefix: "P2_" };

  const linker = createLinker({ windowRef: w, getState: () => state });
  const decorated = (linker.__internal as any).decorateUrlIfNeeded(
    "https://dest.example.test/path",
  );
  const dl = extractDl(decorated);
  assert.ok(dl);
  const decoded = (linker.__internal as any).decodeDl({ value: dl, windowRef: w });
  assert.ok(decoded);
  // applyCookiePrefix avoids double-underscore when prefix ends with "_" and cookie starts with "_"
  assert.ok(decoded.cookies["P1_d8a"]);
  assert.ok(decoded.cookies["P2_d8a"]);
});

test("linker: ignores _dl if too old (ttl) and strips param from URL", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/?x=1",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });
  const state = createRuntimeState();
  state.linker = { domains: ["site-a.example.test"] };
  state.propertyIds = [PID1];
  state.propertyConfigs[PID1] = {};
  const linker = createLinker({ windowRef: w, getState: () => state });

  const realNow = Date.now;
  try {
    // now is far ahead of ts
    Date.now = () => 1700000000000;
    const dl = (linker.__internal as any).encodeDl({
      cookies: { _d8a: "C1.1.999.888" },
      ts: 1700000000000 - 10 * 60 * 1000,
      windowRef: w,
    });
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}&x=1`;
    linker.start();
    assert.equal(jar.get("_d8a"), undefined);
    const href = w.location?.href;
    assert.ok(href, "expected location.href to be set");
    assert.ok(!href.includes("_dl="));
  } finally {
    Date.now = realNow;
  }
});

test("linker: ignores _dl if fingerprint/hash does not match and strips param from URL", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/?x=1",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });
  const state = createRuntimeState();
  state.linker = { domains: ["site-a.example.test"] };
  state.propertyIds = [PID1];
  state.propertyConfigs[PID1] = {};
  const linker = createLinker({ windowRef: w, getState: () => state });

  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const good = (linker.__internal as any).encodeDl({
      cookies: { _d8a: "C1.1.999.888" },
      ts: Date.now(),
      windowRef: w,
    });
    // Corrupt the hash portion (second segment)
    const parts = String(good).split("*");
    parts[1] = `x${parts[1] || ""}`;
    const bad = parts.join("*");
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(bad)}&x=1`;
    linker.start();
    assert.equal(jar.get("_d8a"), undefined);
    const href = w.location?.href;
    assert.ok(href, "expected location.href to be set");
    assert.ok(!href.includes("_dl="));
  } finally {
    Date.now = realNow;
  }
});

test("linker: does not accept incoming when accept_incoming is not enabled (no domains)", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });
  const state = createRuntimeState();
  // accept_incoming defaults false when domains is empty
  state.linker = { domains: [] };
  state.propertyIds = [PID1];
  state.propertyConfigs[PID1] = {};
  const linker = createLinker({ windowRef: w, getState: () => state });

  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const dl = (linker.__internal as any).encodeDl({
      cookies: { _d8a: "C1.1.999.888" },
      ts: Date.now(),
      windowRef: w,
    });
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}`;
    linker.start();
    assert.equal(jar.get("_d8a"), undefined);
  } finally {
    Date.now = realNow;
  }
});

test("linker: accepts incoming when accept_incoming=true even if domains is empty", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });
  const state = createRuntimeState();
  state.linker = { domains: [], accept_incoming: true };
  state.propertyIds = [PID1];
  state.propertyConfigs[PID1] = {};
  const linker = createLinker({ windowRef: w, getState: () => state });

  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const dl = (linker.__internal as any).encodeDl({
      cookies: { _d8a: "C1.1.999.888" },
      ts: Date.now(),
      windowRef: w,
    });
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}`;
    linker.start();
    assert.equal(jar.get("_d8a"), "C1.1.999.888");
  } finally {
    Date.now = realNow;
  }
});

test("linker: does not decorate links when destination host is not in linker.domains", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-a.example.test/",
    hostname: "site-a.example.test",
  });
  const jar = createCookieJar({ windowRef: w });
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
  });

  const state = createRuntimeState();
  state.linker = { domains: ["allowed.example.test"] };
  const linker = createLinker({ windowRef: w, getState: () => state });

  const out = (linker.__internal as any).decorateUrlIfNeeded("https://other.example.test/path");
  assert.ok(!out.includes("_dl="));
});

test("linker: decorate_forms injects hidden input for GET forms (query position)", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-a.example.test/",
    hostname: "site-a.example.test",
  });
  const jar = createCookieJar({ windowRef: w });
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
  });

  // Provide createElement for input injection.
  (w.document as any).createElement = () => {
    const node: any = { attrs: {}, setAttribute: (k: string, v: string) => (node.attrs[k] = v) };
    return node;
  };

  const state = createRuntimeState();
  state.linker = { domains: ["dest.example.test"], decorate_forms: true, url_position: "query" };
  const linker = createLinker({ windowRef: w, getState: () => state });
  linker.start();

  const form: any = {
    tagName: "FORM",
    action: "https://dest.example.test/submit",
    method: "get",
    childNodes: [],
    appendChild: (n: any) => form.childNodes.push(n),
  };

  const submitHandlers = (w as any).__docListeners?.get("submit") || [];
  assert.ok(submitHandlers.length > 0);
  submitHandlers[0]({ target: form });

  const injected = form.childNodes.find((n: any) => n?.attrs?.name === "_dl");
  assert.ok(injected, "expected hidden _dl input");
  assert.ok(String(injected.attrs.value || "").includes("1*"), "expected encoded _dl value");
});

test("linker: url_position=fragment decorates links using fragment", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-a.example.test/",
    hostname: "site-a.example.test",
  });
  const jar = createCookieJar({ windowRef: w });
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
  });

  const state = createRuntimeState();
  state.linker = { domains: ["dest.example.test"], url_position: "fragment" };
  const linker = createLinker({ windowRef: w, getState: () => state });

  const out = (linker.__internal as any).decorateUrlIfNeeded("https://dest.example.test/path");
  assert.ok(out.includes("#"));
  assert.ok(out.includes("_dl="));
  // Ensure it lands in fragment, not query.
  assert.ok(!out.includes("?_dl="));
});

test("linker: multi-runtime pages produce one _dl containing cookies from all runtimes", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-a.example.test/",
    hostname: "site-a.example.test",
  });
  const jar = createCookieJar({ windowRef: w });

  // Runtime 1 cookies (prop1_)
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookiePrefix: "prop1_",
  });
  ensureSession({
    jar,
    propertyId: PID1,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookiePrefix: "prop1_",
    sessionTimeoutMs: 30 * 60 * 1000,
  });

  // Runtime 2 cookies (prop2_)
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookiePrefix: "prop2_",
  });
  ensureSession({
    jar,
    propertyId: PID2,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookiePrefix: "prop2_",
    sessionTimeoutMs: 30 * 60 * 1000,
  });

  const state1 = createRuntimeState();
  state1.linker = { domains: ["dest.example.test"] };
  state1.propertyIds = [PID1];
  state1.propertyConfigs[PID1] = { cookie_prefix: "prop1_" };

  const state2 = createRuntimeState();
  state2.propertyIds = [PID2];
  state2.propertyConfigs[PID2] = { cookie_prefix: "prop2_" };

  // Register both runtimes against the same window.
  const linker1 = createLinker({ windowRef: w, getState: () => state1 });
  createLinker({ windowRef: w, getState: () => state2 });

  const decorated = (linker1.__internal as any).decorateUrlIfNeeded(
    "https://dest.example.test/path",
  );
  const dl = extractDl(decorated);
  assert.ok(dl);

  const decoded = (linker1.__internal as any).decodeDl({ value: dl, windowRef: w });
  assert.ok(decoded);

  // Both prefixed client cookies should be present.
  assert.ok(decoded.cookies.prop1_d8a);
  assert.ok(decoded.cookies.prop2_d8a);
  // Both prefixed session cookies should be present.
  assert.ok(decoded.cookies[`prop1_d8a_${PID1}`]);
  assert.ok(decoded.cookies[`prop2_d8a_${PID2}`]);
});

test("linker: incoming _dl is applied after config becomes available (do not clear too early)", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/?x=1",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });

  const state = createRuntimeState();
  const linker = createLinker({ windowRef: w, getState: () => state });

  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const dl = (linker.__internal as any).encodeDl({
      cookies: {
        prop1_d8a: "C1.1.999.888",
        [`prop1_d8a_${PID1}`]: "S1.1.s1$o1$g0$t1$j0$dnew",
      },
      ts: Date.now(),
      windowRef: w,
    });

    // Start early: parse incoming from URL, but config is not known yet.
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}&x=1`;
    linker.start();

    // Enable accept_incoming (simulates later processing of set('linker', ...)).
    linker.updateConfigPatch({ domains: ["site-a.example.test"] });
    linker.applyIncomingIfReady();

    // Still no cookie spec for prop1_ until the property is configured.
    assert.equal(jar.get("prop1_d8a"), undefined);

    // Now config becomes available (cookie prefix + property id).
    state.propertyIds = [PID1];
    state.propertyConfigs[PID1] = { cookie_prefix: "prop1_" };

    // Should now apply the previously parsed incoming payload.
    linker.applyIncomingIfReady();
    assert.equal(jar.get("prop1_d8a"), "C1.1.999.888");
    assert.ok(jar.get(`prop1_d8a_${PID1}`));
  } finally {
    Date.now = realNow;
  }
});

test("linker: incoming _dl is applied incrementally as more properties become configured", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/?x=1",
    hostname: "site-b.example.test",
  });
  const jar = createCookieJar({ windowRef: w });

  const state1 = createRuntimeState();
  const state2 = createRuntimeState();
  const linker1 = createLinker({ windowRef: w, getState: () => state1 });
  createLinker({ windowRef: w, getState: () => state2 });

  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const dl = (linker1.__internal as any).encodeDl({
      cookies: {
        prop1_d8a: "C1.1.999.888",
        [`prop1_d8a_${PID1}`]: "S1.1.s1$o1$g0$t1$j0$dnew",
        prop2_d8a: "C1.1.777.666",
        [`prop2_d8a_${PID2}`]: "S1.1.s2$o1$g0$t2$j0$dnew",
      },
      ts: Date.now(),
      windowRef: w,
    });

    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}&x=1`;
    linker1.start();

    // Enable accept_incoming via domains (default accept_incoming=true when domains present).
    linker1.updateConfigPatch({ domains: ["site-a.example.test"] });

    // Configure only property 1 first.
    state1.propertyIds = [PID1];
    state1.propertyConfigs[PID1] = { cookie_prefix: "prop1_" };

    linker1.applyIncomingIfReady();
    assert.equal(jar.get("prop1_d8a"), "C1.1.999.888");
    assert.ok(jar.get(`prop1_d8a_${PID1}`));
    // prop2 should not yet be applied.
    assert.equal(jar.get("prop2_d8a"), undefined);

    // Later, property 2 config becomes available in a different runtime instance.
    state2.propertyIds = [PID2];
    state2.propertyConfigs[PID2] = { cookie_prefix: "prop2_" };

    linker1.applyIncomingIfReady();
    assert.equal(jar.get("prop2_d8a"), "C1.1.777.666");
    assert.ok(jar.get(`prop2_d8a_${PID2}`));
  } finally {
    Date.now = realNow;
  }
});

test("linker: applying incoming session seeds shared session state (prevents overwrite on first page_view)", () => {
  const w = makeWindowWithCookieStore({
    href: "https://site-b.example.test/?x=1",
    hostname: "site-b.example.test",
  });
  const state = createRuntimeState();
  const linker = createLinker({ windowRef: w, getState: () => state });

  const incoming = "S1.1.s1768057097$o12$g1$t1768059837$j204$dvwbd4utkbyq5ygwb0n";
  const realNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    const dl = (linker.__internal as any).encodeDl({
      cookies: {
        prop1_d8a: "C1.1.999.888",
        [`prop1_d8a_${PID1}`]: incoming,
      },
      ts: Date.now(),
      windowRef: w,
    });
    w.location!.href = `https://site-b.example.test/?_dl=${encodeURIComponent(dl)}&x=1`;
    linker.start();
    linker.updateConfigPatch({ domains: ["site-a.example.test"] });

    state.propertyIds = [PID1];
    state.propertyConfigs[PID1] = { cookie_prefix: "prop1_" };

    linker.applyIncomingIfReady();

    assert.ok(
      Array.isArray(state.sharedSessionTokens),
      "expected sharedSessionTokens to be seeded",
    );
    assert.equal(state.sharedSessionValue, incoming);
  } finally {
    Date.now = realNow;
  }
});
