import test from "node:test";
import assert from "node:assert/strict";

import { attachD8aCollectMetadata } from "../src/utils/collect_metadata.ts";

test("attachD8aCollectMetadata sets _dtv and _dtn on params", () => {
  const params = new URLSearchParams();
  attachD8aCollectMetadata(params);
  assert.ok(params.get("_dtv"), "_dtv should be set");
  assert.equal(params.get("_dtn"), "wt", "_dtn should be web-tracker name - wt");
});
