import test from "node:test";
import assert from "node:assert/strict";

import { createCookieJar } from "../src/cookies/cookie_jar.ts";
import { getCookieDomainCandidates } from "../src/utils/gtag_primitives.ts";

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

test("cookie jar: auto domain picks broadest first (example.test before docs.example.test)", () => {
  const document = makeDocumentCookieWithPolicy({
    acceptDomains: [".example.test", ".docs.example.test"],
  });
  const w = { document, location: { hostname: "docs.example.test", protocol: "https:" } };
  const jar = createCookieJar({ windowRef: w });

  const res = jar.set("_d8a", "C1.1.1.2", { domain: "auto" });
  assert.equal(res.ok, true);
  assert.equal(res.domain, ".example.test");
  assert.match(document.__lastCookieWrite, /domain=\.example\.test/i);
});

test("cookie jar: auto domain falls back to host-only when no domain candidates accepted", () => {
  const document = makeDocumentCookieWithPolicy({
    acceptDomains: [], // reject any domain=...
  });
  const w = { document, location: { hostname: "docs.example.test", protocol: "https:" } };
  const jar = createCookieJar({ windowRef: w });

  const res = jar.set("_d8a", "C1.1.1.2", { domain: "auto" });
  assert.equal(res.ok, true);
  assert.equal(res.domain, null);
  assert.ok(!/domain=/i.test(document.__lastCookieWrite));
});

test("cookie jar: http + SameSite=None is downgraded to Lax (dev-friendly)", () => {
  const document = makeDocumentCookieWithPolicy({
    acceptDomains: ["none"], // irrelevant for this test
  });
  const w = { document, location: { hostname: "localhost", protocol: "http:" } };
  const jar = createCookieJar({ windowRef: w });

  const res = jar.set("_d8a", "C1.1.1.2", { domain: "auto", sameSite: "None", secure: true });
  assert.equal(res.ok, true);
  assert.match(document.__lastCookieWrite.toLowerCase(), /samesite=lax/);
  // secure is ignored on http pages
  assert.ok(!document.__lastCookieWrite.toLowerCase().includes("secure"));
});

test("getCookieDomainCandidates: docs.example.test", () => {
  assert.deepEqual(getCookieDomainCandidates("docs.example.test"), [
    "example.test",
    "docs.example.test",
    "none",
  ]);
});

test("getCookieDomainCandidates: example.test", () => {
  assert.deepEqual(getCookieDomainCandidates("example.test"), ["example.test", "none"]);
});

test("getCookieDomainCandidates: empty", () => {
  assert.deepEqual(getCookieDomainCandidates(""), ["none"]);
});
