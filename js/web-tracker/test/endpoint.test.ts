import test from "node:test";
import assert from "node:assert/strict";

import { resolveTrackingUrl } from "../src/utils/endpoint.ts";

test("resolveTrackingUrl: default", () => {
  assert.throws(() => resolveTrackingUrl(), /server_container_url is required/);
});

test("resolveTrackingUrl: base domain", () => {
  assert.equal(
    resolveTrackingUrl({ server_container_url: "https://example.org/d/c" }),
    "https://example.org/d/c",
  );
  assert.equal(
    resolveTrackingUrl({ server_container_url: "https://example.org/d/c/" }),
    "https://example.org/d/c",
  );
});

test("resolveTrackingUrl: does not force a /d/c path", () => {
  assert.equal(
    resolveTrackingUrl({ server_container_url: "https://example.org/custom/path" }),
    "https://example.org/custom/path",
  );
  assert.equal(
    resolveTrackingUrl({ server_container_url: "https://example.org/custom/path/" }),
    "https://example.org/custom/path",
  );
});
