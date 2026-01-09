import type { ConsentState } from "../types.ts";

function encodeConsentStateBit(a: unknown) {
  // Encode consent state into the `gcs` bit representation used by the Google tag / GA4 collect
  // wire format:
  // - 1 => "1"
  // - 2 or 4 => "0"
  // - else => "-"
  switch (a) {
    case 1:
      return "1";
    case 2:
    case 4:
      return "0";
    default:
      return "-";
  }
}

function consentStatusToQl(status: unknown) {
  // Map consent status to the internal states used by `buildGcs`.
  // - granted => 1 ("1")
  // - denied => 2 ("0")
  // - undefined => 1 (treat as granted by default in our tracker)
  const s = String(status || "").toLowerCase();
  if (s === "denied") return 2;
  return 1;
}

/**
 * Build `gcs` in the shape expected by the Google tag / GA4 collect wire format:
 * `G1` + encode(ad_storage_state) + encode(analytics_storage_state)
 */
export function buildGcs({ consent }: { consent: ConsentState }) {
  const c = consent || {};
  const ad = consentStatusToQl(c.ad_storage);
  const an = consentStatusToQl(c.analytics_storage);
  return `G1${encodeConsentStateBit(ad)}${encodeConsentStateBit(an)}`;
}

/**
 * Build `gcd` based on Consent Mode v2 state transitions.
 *
 * Uses the letter meanings described in Simo Ahava’s Consent Mode v2 guide:
 * - `l`: not set
 * - `p`: denied by default (no update)
 * - `q`: denied by default and after update
 * - `t`: granted by default (no update)
 * - `r`: denied by default, granted after update
 * - `m`: denied after update (no default)
 * - `n`: granted after update (no default)
 * - `u`: granted by default, denied after update
 * - `v`: granted by default and after update
 *
 * Reference: https://www.simoahava.com/analytics/consent-mode-v2-google-tags/
 *
 * Note: The official `gcd` encoding contains more metadata than just the
 * consent transitions. For our purposes we encode the transition letters for
 * the 4 main consent signals and preserve the observed framing:
 * - prefix: `13`
 * - suffix: `l1`
 *
 * This ensures the known baselines match:
 * - all denied (default denied, no update): `13p3p3p2p5l1`
 * - all granted (default denied, update granted): `13r3r3r2r5l1`
 */
export function buildGcd({
  consentDefault,
  consentUpdate,
}: {
  consentDefault: ConsentState;
  consentUpdate: ConsentState;
}) {
  const d = consentDefault || {};
  const u = consentUpdate || {};

  function status(v: unknown) {
    const s = String(v || "").toLowerCase();
    if (s === "granted") return "granted";
    if (s === "denied") return "denied";
    return null;
  }

  function letterFor(def: unknown, upd: unknown) {
    const defS = status(def);
    const updS = status(upd);

    // no default, no update
    if (!defS && !updS) return "l";

    // default only
    if (defS === "denied" && !updS) return "p";
    if (defS === "granted" && !updS) return "t";

    // update only
    if (!defS && updS === "denied") return "m";
    if (!defS && updS === "granted") return "n";

    // both present
    if (defS === "denied" && updS === "denied") return "q";
    if (defS === "granted" && updS === "granted") return "v";
    if (defS === "denied" && updS === "granted") return "r";
    if (defS === "granted" && updS === "denied") return "u";

    return "l";
  }

  // The digits after each letter match the observed Consent Mode v2 wire strings.
  const parts: Array<[string, string]> = [
    ["ad_storage", "3"],
    ["analytics_storage", "3"],
    ["ad_user_data", "2"],
    ["ad_personalization", "5"],
  ];

  let out = "13";
  for (const [k, digit] of parts) {
    out += `${letterFor(d[k], u[k])}${digit}`;
  }
  out += "l1";
  return out;
}
