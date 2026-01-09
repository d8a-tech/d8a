import test from "node:test";
import assert from "node:assert/strict";

import { resolveGtagDataLayerName } from "../src/runtime/runtime_names.ts";
import { makeWindowMock } from "./test-utils.ts";

test("resolveGtagDataLayerName: default is dataLayer", () => {
  const w = makeWindowMock({ document: { cookie: "" } });
  assert.equal(resolveGtagDataLayerName({ windowRef: w }), "dataLayer");
});

test("resolveGtagDataLayerName: respects explicit override", () => {
  const w = makeWindowMock({ document: { cookie: "" } });
  assert.equal(
    resolveGtagDataLayerName({ windowRef: w, gtagDataLayerName: "myGtagLayer" }),
    "myGtagLayer",
  );
});

test("resolveGtagDataLayerName: reads from window.d8aGtagDataLayerName", () => {
  const w = makeWindowMock({ document: { cookie: "" }, d8aGtagDataLayerName: "myGtagLayer" });
  assert.equal(resolveGtagDataLayerName({ windowRef: w }), "myGtagLayer");
});

test("resolveGtagDataLayerName: reads from currentScript ?gl=", () => {
  const w = makeWindowMock({
    document: {
      cookie: "",
      currentScript: {
        src: "https://cdn.example.org/web-tracker.js?gl=myGtagLayer&g=d8a2&l=d8aLayer2",
      },
    },
    location: { href: "https://example.org/" },
  });
  assert.equal(resolveGtagDataLayerName({ windowRef: w }), "myGtagLayer");
});
