import test from "node:test";
import assert from "node:assert/strict";

import {
  buildD8aCookieName,
  parseD8aClientCookie,
  parseD8aSessionCookie,
  serializeD8aSessionCookieTokens,
  updateD8aSessionCookieTokens,
} from "../src/cookies/d8a_cookies.ts";

test("buildD8aCookieName", () => {
  assert.equal(
    buildD8aCookieName("80e1d6d0-560d-419f-ac2a-fe9281e93386"),
    "_d8a_80e1d6d0-560d-419f-ac2a-fe9281e93386",
  );
});

test("parseD8aClientCookie", () => {
  assert.deepEqual(parseD8aClientCookie("C1.1.457554353.1762098975"), {
    cid: "457554353.1762098975",
  });
  assert.equal(parseD8aClientCookie("bad"), null);
});

test("session cookie parse/serialize roundtrip keeps prefix", () => {
  const raw = "S1.1.s1767124410$o71$g1$t1767129307$j59$dabc";
  const parsed = parseD8aSessionCookie(raw);
  assert.ok(parsed);
  const out = serializeD8aSessionCookieTokens(parsed.tokens);
  assert.equal(out, raw);
});

test("session update: creates new when missing", () => {
  const { tokens, isNewSession } = updateD8aSessionCookieTokens(null, { nowMs: 1767124410000 });
  assert.equal(isNewSession, true);
  const out = serializeD8aSessionCookieTokens(tokens);
  assert.match(out, /^S1\.1\./);
});

test("session update: increments j and updates t within same session", () => {
  const raw = "S1.1.s1767124410$o7$g0$t1767124410$j2$dabc";
  const parsed = parseD8aSessionCookie(raw);
  assert.ok(parsed);
  const updated = updateD8aSessionCookieTokens(parsed.tokens, {
    nowMs: 1767124470000,
    sessionTimeoutMs: 30 * 60 * 1000,
  });
  assert.equal(updated.isNewSession, false);
  const out = serializeD8aSessionCookieTokens(updated.tokens);
  assert.match(out, /\$j3(\$|$)/); // j incremented
  assert.match(out, /\$t1767124470(\$|$)/); // t updated
});

test("session update: times out and increments o", () => {
  const raw = "S1.1.s1767124410$o7$g1$t1767124410$j2$dabc";
  const parsed = parseD8aSessionCookie(raw);
  assert.ok(parsed);
  const updated = updateD8aSessionCookieTokens(parsed.tokens, {
    nowMs: 1767124410000 + 31 * 60 * 1000,
    sessionTimeoutMs: 30 * 60 * 1000,
  });
  assert.equal(updated.isNewSession, true);
  const out = serializeD8aSessionCookieTokens(updated.tokens);
  assert.match(out, /\$o8(\$|$)/);
});
