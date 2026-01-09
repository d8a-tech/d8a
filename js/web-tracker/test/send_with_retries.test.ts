import test from "node:test";
import assert from "node:assert/strict";

import { sendWithRetries } from "../src/transport/send.ts";

test("sendWithRetries: retries once on network error then succeeds (default maxRetries=1)", async () => {
  const calls: number[] = [];
  const w = {
    navigator: { sendBeacon: null },
    fetch: async () => {
      calls.push(1);
      if (calls.length === 1) throw new Error("network down");
      return { status: 0 }; // opaque/no-cors style => treated as success
    },
  };

  const res = await sendWithRetries({
    url: "https://example.test/g/collect?x=1",
    windowRef: w,
    useBeacon: false,
    initialBackoffMs: 0,
  });

  assert.equal(calls.length, 2);
  assert.equal(res.ok, true);
  assert.equal(res.via, "fetch");
});

test("sendWithRetries: retries once on 5xx then succeeds", async () => {
  const calls: number[] = [];
  const w = {
    navigator: { sendBeacon: null },
    fetch: async () => {
      calls.push(1);
      if (calls.length === 1) return { status: 500 };
      return { status: 204 };
    },
  };

  const res = await sendWithRetries({
    url: "https://example.test/g/collect?x=1",
    windowRef: w,
    useBeacon: false,
    initialBackoffMs: 0,
  });

  assert.equal(calls.length, 2);
  assert.equal(res.ok, true);
  assert.equal(res.via, "fetch");
  assert.equal(res.status, 204);
});

test("sendWithRetries: does not retry on 404", async () => {
  const calls: number[] = [];
  const w = {
    navigator: { sendBeacon: null },
    fetch: async () => {
      calls.push(1);
      return { status: 404 };
    },
  };

  const res = await sendWithRetries({
    url: "https://example.test/g/collect?x=1",
    windowRef: w,
    useBeacon: false,
    initialBackoffMs: 0,
  });

  assert.equal(calls.length, 1);
  assert.equal(res.ok, true);
  assert.equal(res.via, "fetch");
  assert.equal(res.status, 404);
});
