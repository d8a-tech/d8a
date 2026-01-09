import test from "node:test";
import assert from "node:assert/strict";

import { fetchUach, getUachCached, uachToParams } from "../src/runtime/uach.ts";
import type { WindowLike } from "../src/types.ts";

test("uach: fetches high entropy values once and caches", async () => {
  let calls = 0;
  const w: WindowLike = {
    document: { cookie: "" },
    setTimeout: () => 1,
    clearTimeout: () => {},
    d8a_tag_data: {},
    navigator: {
      userAgentData: {
        getHighEntropyValues: async () => {
          calls += 1;
          return {
            architecture: "arm",
            bitness: "64",
            fullVersionList: [{ brand: "Chromium", version: "142.0.0.0" }],
            mobile: false,
            model: "",
            platform: "macOS",
            platformVersion: "26.1.0",
            wow64: false,
          };
        },
      },
    },
  };

  const p1 = fetchUach(w);
  const p2 = fetchUach(w);
  assert.equal(p1, p2);

  const v = await p1!;
  assert.equal(calls, 1);
  assert.ok(getUachCached(w));

  const params = uachToParams(v)!;
  assert.equal(params.uaa, "arm");
  assert.equal(params.uab, "64");
  assert.equal(params.uamb, "0");
  assert.equal(params.uap, "macOS");
  assert.equal(params.uapv, "26.1.0");
  assert.equal(params.uaw, "0");
  assert.match(params.uafvl, /^Chromium;142\.0\.0\.0$/);
});
