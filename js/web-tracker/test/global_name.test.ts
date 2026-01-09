import test from "node:test";
import assert from "node:assert/strict";

import { resolveGlobalName } from "../src/runtime/runtime_names.ts";
import { installD8a } from "../src/install.ts";
import { makeWindowMock } from "./test-utils.ts";

test("resolveGlobalName: default is d8a", () => {
  const w = makeWindowMock({ document: { cookie: "" } });
  assert.equal(resolveGlobalName({ windowRef: w }), "d8a");
});

test("resolveGlobalName: respects explicit override", () => {
  const w = makeWindowMock({ document: { cookie: "" } });
  assert.equal(resolveGlobalName({ windowRef: w, globalName: "d8a2" }), "d8a2");
});

test("resolveGlobalName: reads from currentScript ?g=", () => {
  const w = makeWindowMock({
    d8aGlobalName: "",
    document: {
      cookie: "",
      currentScript: { src: "https://cdn.example.org/web-tracker.js?g=d8a2&l=d8aLayer2" },
    },
    location: { href: "https://example.org/" },
  });
  assert.equal(resolveGlobalName({ windowRef: w }), "d8a2");
});

test("installD8a: does not overwrite an existing custom global", () => {
  const w = makeWindowMock({
    location: { hostname: "example.org", href: "https://example.org/" },
    document: {
      cookie: "",
      title: "T",
      referrer: "",
      currentScript: { src: "https://cdn.example.org/web-tracker.js?g=d8a2&l=d8aLayer2" },
    },
    fetch: async () => ({ status: 0 }),
  }) as ReturnType<typeof makeWindowMock> & {
    d8aLayer2: unknown[];
    d8a2: () => void;
  };
  w.d8aLayer2 = [];
  w.d8a2 = function existing() {};

  const { globalName, dataLayerName } = installD8a({ windowRef: w });
  assert.equal(globalName, "d8a2");
  assert.equal(dataLayerName, "d8aLayer2");
  assert.equal(w.d8a2.name, "existing");
});
