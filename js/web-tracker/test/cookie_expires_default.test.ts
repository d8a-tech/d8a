import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { PROPERTY_ID, makeWindowMock, getD8a } from "./test-utils.ts";

function installCookieStoreWithPolicy(w: ReturnType<typeof makeWindowMock>) {
  const store = new Map<string, string>();
  const writes: string[] = [];
  let lastCookieWrite = "";

  Object.defineProperty(w.document, "cookie", {
    configurable: true,
    get: () =>
      Array.from(store.entries())
        .map(([k, v]) => `${k}=${v}`)
        .join("; "),
    set: (v: unknown) => {
      lastCookieWrite = String(v);
      writes.push(lastCookieWrite);
      const parts = String(v)
        .split(";")
        .map((s) => s.trim());
      const [nameValue, ...attrs] = parts;
      const idx = nameValue.indexOf("=");
      if (idx < 0) return;
      const name = nameValue.slice(0, idx);
      const value = nameValue.slice(idx + 1);

      // expires delete
      const expiresAttr = attrs.find((a) => a.toLowerCase().startsWith("expires="));
      if (expiresAttr && expiresAttr.toLowerCase().includes("thu, 01 jan 1970")) {
        store.delete(name);
        return;
      }

      // Max-Age=0 deletes immediately
      const maxAgeAttr = attrs.find((a) => a.toLowerCase().startsWith("max-age="));
      if (maxAgeAttr) {
        const vRaw = maxAgeAttr.split("=").slice(1).join("=").trim();
        if (String(Number(vRaw)) === vRaw && Number(vRaw) <= 0) {
          store.delete(name);
          return;
        }
      }

      store.set(name, value);
    },
  });

  // Attach introspection helpers to the mock document
  (w.document as any).__lastCookieWrite = () => lastCookieWrite;
  (w.document as any).__writes = () => writes.slice();
}

function makeWindow() {
  const w = makeWindowMock({
    location: { href: "http://mydev.com/example/", hostname: "mydev.com", protocol: "http:" },
    navigator: { language: "en-gb" },
  });
  installCookieStoreWithPolicy(w);
  return w;
}

test("cookie_expires default: does not emit Max-Age=0 (cookies persist)", () => {
  const w = makeWindow();
  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  // No cookie_expires set here; before the fix, this defaulted to null -> Max-Age=0.
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    cookie_domain: "none",
    cookie_flags: "SameSite=Lax",
  });
  d8a("event", "page_view", {});

  const lastWrite = (w.document as any).__lastCookieWrite();
  assert.ok(!/max-age=0/i.test(lastWrite), "should not write Max-Age=0");
  assert.match(w.document.cookie, /(^|;\s*)_d8a=/, "should create _d8a");
  assert.match(
    w.document.cookie,
    new RegExp(`(^|;\\s*)_d8a_${PROPERTY_ID}=`),
    `should create _d8a_${PROPERTY_ID}`,
  );
});

test("cookie_expires default: _d8a_G-<property> has same default lifetime as _d8a (2y)", () => {
  const w = makeWindow();
  installD8a({ windowRef: w });
  const d8a = getD8a(w);

  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    cookie_domain: "none",
    cookie_flags: "SameSite=Lax",
  });
  d8a("event", "page_view", {});

  const writes = (w.document as any).__writes();
  const clientWrite = writes.find((s: string) => s.startsWith("_d8a=")) || "";
  const sessionWrite = writes.find((s: string) => s.startsWith(`_d8a_${PROPERTY_ID}=`)) || "";

  assert.ok(clientWrite, "expected a _d8a cookie write");
  assert.ok(sessionWrite, `expected a _d8a_${PROPERTY_ID} cookie write`);

  // 2 years in seconds
  assert.match(clientWrite.toLowerCase(), /max-age=63072000/, "_d8a should default to 2y Max-Age");
  assert.match(
    sessionWrite.toLowerCase(),
    /max-age=63072000/,
    "_d8a_<property_id> should default to 2y Max-Age",
  );
});
