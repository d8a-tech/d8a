import test from "node:test";
import assert from "node:assert/strict";

import { buildGcs, buildGcd } from "../src/ga4/consent_wire.ts";

test("gcs: granted/granted => G111", () => {
  assert.equal(
    buildGcs({ consent: { ad_storage: "granted", analytics_storage: "granted" } }),
    "G111",
  );
});

test("gcs: denied/denied => G100", () => {
  assert.equal(
    buildGcs({ consent: { ad_storage: "denied", analytics_storage: "denied" } }),
    "G100",
  );
});

test("gcd: is stable and starts with version prefix", () => {
  // all declined baseline
  assert.equal(
    buildGcd({
      consentDefault: {
        ad_storage: "denied",
        analytics_storage: "denied",
        ad_user_data: "denied",
        ad_personalization: "denied",
      },
      consentUpdate: {},
    }),
    "13p3p3p2p5l1",
  );

  // all granted baseline (default denied, then updated to granted)
  assert.equal(
    buildGcd({
      consentDefault: {
        ad_storage: "denied",
        analytics_storage: "denied",
        ad_user_data: "denied",
        ad_personalization: "denied",
      },
      consentUpdate: {
        ad_storage: "granted",
        analytics_storage: "granted",
        ad_user_data: "granted",
        ad_personalization: "granted",
      },
    }),
    "13r3r3r2r5l1",
  );
});

test("gcd: encodes default/update transitions using consent-mode-v2 letter semantics", () => {
  // This test follows the transition letter meanings described in:
  // https://www.simoahava.com/analytics/consent-mode-v2-google-tags/
  //
  // Note: we use a single consent type (ad_storage) to validate the letter,
  // and we keep all other signals unset to make the output easy to read:
  // `13<ad_storage_letter>3 l3 l2 l5 l1`
  const baselineTail = "l3l2l5l1";

  assert.equal(buildGcd({ consentDefault: {}, consentUpdate: {} }), `13l3${baselineTail}`);

  // default only
  assert.equal(
    buildGcd({ consentDefault: { ad_storage: "denied" }, consentUpdate: {} }),
    `13p3${baselineTail}`,
  );
  assert.equal(
    buildGcd({ consentDefault: { ad_storage: "granted" }, consentUpdate: {} }),
    `13t3${baselineTail}`,
  );

  // update only
  assert.equal(
    buildGcd({ consentDefault: {}, consentUpdate: { ad_storage: "denied" } }),
    `13m3${baselineTail}`,
  );
  assert.equal(
    buildGcd({ consentDefault: {}, consentUpdate: { ad_storage: "granted" } }),
    `13n3${baselineTail}`,
  );

  // both present
  assert.equal(
    buildGcd({ consentDefault: { ad_storage: "denied" }, consentUpdate: { ad_storage: "denied" } }),
    `13q3${baselineTail}`,
  );
  assert.equal(
    buildGcd({
      consentDefault: { ad_storage: "granted" },
      consentUpdate: { ad_storage: "granted" },
    }),
    `13v3${baselineTail}`,
  );
  assert.equal(
    buildGcd({
      consentDefault: { ad_storage: "denied" },
      consentUpdate: { ad_storage: "granted" },
    }),
    `13r3${baselineTail}`,
  );
  assert.equal(
    buildGcd({
      consentDefault: { ad_storage: "granted" },
      consentUpdate: { ad_storage: "denied" },
    }),
    `13u3${baselineTail}`,
  );
});
