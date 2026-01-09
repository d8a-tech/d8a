import test from "node:test";
import assert from "node:assert/strict";

import { createCookieJar } from "../src/cookies/cookie_jar.ts";
import { ensureClientId, ensureSession } from "../src/cookies/identity.ts";
import { PROPERTY_ID } from "./test-utils.ts";

function makeDocumentCookieWithPolicy({ acceptDomains }: { acceptDomains: string[] }) {
  const store = new Map<string, string>();
  const policyAccept = new Set(acceptDomains);
  let lastCookieWrite = "";

  return {
    get cookie() {
      return Array.from(store.entries())
        .map(([k, v]) => `${k}=${v}`)
        .join("; ");
    },
    set cookie(v) {
      lastCookieWrite = String(v);
      const parts = String(v)
        .split(";")
        .map((s) => s.trim());
      const [nameValue, ...attrs] = parts;
      const idx = nameValue.indexOf("=");
      if (idx < 0) return;
      const name = nameValue.slice(0, idx);
      const value = nameValue.slice(idx + 1);

      const domainAttr = attrs.find((a) => a.toLowerCase().startsWith("domain="));
      const domain = domainAttr ? domainAttr.split("=").slice(1).join("=").toLowerCase() : null;

      // policy: if domain attribute present, accept only when listed
      if (domain) {
        if (!policyAccept.has(domain)) return;
      }

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
    get __lastCookieWrite() {
      return lastCookieWrite;
    },
  };
}

test("ensureClientId: denied consent does not write", () => {
  const document = makeDocumentCookieWithPolicy({ acceptDomains: [".example.test"] });
  const w = { document, location: { hostname: "docs.example.test", protocol: "https:" } };
  const jar = createCookieJar({ windowRef: w });

  const res = ensureClientId({
    jar,
    consent: { analytics_storage: "denied" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
  });

  assert.ok(res.cid);
  assert.equal(res.wrote, false);
  assert.equal(jar.get("_d8a"), undefined);
});

test("ensureSession: denied consent does not write", () => {
  const document = makeDocumentCookieWithPolicy({ acceptDomains: [".example.test"] });
  const w = { document, location: { hostname: "docs.example.test", protocol: "https:" } };
  const jar = createCookieJar({ windowRef: w });

  const res = ensureSession({
    jar,
    propertyId: PROPERTY_ID,
    consent: { analytics_storage: "denied" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    sessionTimeoutMs: 30 * 60 * 1000,
  });

  assert.ok(res.sid);
  assert.ok(res.sct);
  assert.equal(res.wrote, false);
  assert.equal(jar.get(`_d8a_${PROPERTY_ID}`), undefined);
});

test("ensureClientId/ensureSession: cookie_prefix is applied to cookie names", () => {
  const document = makeDocumentCookieWithPolicy({ acceptDomains: [".example.test"] });
  const w = { document, location: { hostname: "docs.example.test", protocol: "https:" } };
  const jar = createCookieJar({ windowRef: w });

  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookiePrefix: "TEST_PREFIX_",
  });
  ensureSession({
    jar,
    propertyId: PROPERTY_ID,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookiePrefix: "TEST_PREFIX_",
    sessionTimeoutMs: 30 * 60 * 1000,
  });

  // Note: prefixing collapses '_' boundary to avoid double underscores.
  assert.ok(jar.get("TEST_PREFIX_d8a"));
  assert.ok(jar.get(`TEST_PREFIX_d8a_${PROPERTY_ID}`));
});

test("ensureSession: cookie_update=false does not refresh existing session cookie", () => {
  const document = makeDocumentCookieWithPolicy({ acceptDomains: [".example.test"] });
  const w = { document, location: { hostname: "docs.example.test", protocol: "https:" } };
  const jar = createCookieJar({ windowRef: w });

  // create once
  ensureSession({
    jar,
    propertyId: PROPERTY_ID,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    sessionTimeoutMs: 30 * 60 * 1000,
  });
  const firstWrite = document.__lastCookieWrite;

  // attempt refresh with cookie_update=false
  ensureSession({
    jar,
    propertyId: PROPERTY_ID,
    consent: { analytics_storage: "granted" },
    nowMs: 1767129999000,
    cookieDomain: "auto",
    cookieUpdate: false,
    sessionTimeoutMs: 30 * 60 * 1000,
  });
  const secondWrite = document.__lastCookieWrite;

  assert.equal(secondWrite, firstWrite);
});

test("ensureClientId: updates existing cookie attributes when cookie_update is enabled", () => {
  const document = makeDocumentCookieWithPolicy({ acceptDomains: [".example.test"] });
  const w = { document, location: { hostname: "docs.example.test", protocol: "https:" } };
  const jar = createCookieJar({ windowRef: w });

  // create once (default attrs)
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
  });
  const before = document.__lastCookieWrite;

  // update attrs without deleting cookie
  const res = ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookieSameSite: "Strict",
    cookieSecure: true,
  });

  assert.ok(res.cid);
  assert.notEqual(document.__lastCookieWrite, before);
  assert.match(document.__lastCookieWrite.toLowerCase(), /samesite=strict/);
  // on https, secure should be present
  assert.match(document.__lastCookieWrite.toLowerCase(), /(^|;\s*)secure(\s*;|$)/);
});

test("ensureClientId: updates existing cookie attributes even when cookie_update=false (security change)", () => {
  const document = makeDocumentCookieWithPolicy({ acceptDomains: [".example.test"] });
  const w = { document, location: { hostname: "docs.example.test", protocol: "https:" } };
  const jar = createCookieJar({ windowRef: w });

  // create once
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
  });
  const before = document.__lastCookieWrite;

  // apply new attrs while cookie_update=false
  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "auto",
    cookieUpdate: false,
    forceCookieAttrsWrite: true,
    cookieSameSite: "Strict",
  });

  assert.notEqual(document.__lastCookieWrite, before);
  assert.match(document.__lastCookieWrite.toLowerCase(), /samesite=strict/);
});

test("cookie_flags: Secure does not prevent setting cookies on http pages", () => {
  const document = makeDocumentCookieWithPolicy({ acceptDomains: [".mydev.com"] });
  const w = { document, location: { hostname: "mydev.com", protocol: "http:" } };
  const jar = createCookieJar({ windowRef: w });

  ensureClientId({
    jar,
    consent: { analytics_storage: "granted" },
    nowMs: 1767124410000,
    cookieDomain: "mydev.com",
    cookieSameSite: "Strict",
    cookieSecure: true,
  });

  // We should still set a cookie; secure is ignored on http for dev.
  assert.ok(jar.get("_d8a"));
  assert.doesNotMatch(document.__lastCookieWrite.toLowerCase(), /(^|;\s*)secure(\s*;|$)/);
});
