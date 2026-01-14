import test from "node:test";
import assert from "node:assert/strict";

import { encodeItemToPrValue, buildGa4CollectQueryParams } from "../src/ga4/gtag_mapper.ts";

function getDefaultBuildParams() {
  return {
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    eventParams: {} as Record<string, unknown>,
    cookieHeader: "",
    clientId: null as string | null,
    userId: null as string | null,
    cookiePrefix: "",
    ignoreReferrer: false,
    browser: {
      dl: "https://example.test/",
      dt: "My Dev",
      dh: "example.test",
      dr: "",
      ul: "en-gb",
      sr: "5120x1440",
    },
    pageLoadMs: null as number | null,
    hitCounter: null as number | null,
    engagementTimeMs: null as number | null,
    uachParams: null as Record<string, string> | null,
    campaign: null as {
      campaign_id?: string | null;
      campaign_source?: string | null;
      campaign_medium?: string | null;
      campaign_name?: string | null;
      campaign_term?: string | null;
      campaign_content?: string | null;
    } | null,
    contentGroup: null as string | null,
    consentParams: null as { gcs?: string | null; gcd?: string | null; consent?: any } | null,
    debugMode: false,
  };
}

test("encodeItemToPrValue: encodes known fields + custom params as kN/vN", () => {
  const item = {
    item_id: "SKU_12346",
    item_name: "Google Grey Women's Tee",
    affiliation: "Google Merchandise Store",
    coupon: "SUMMER_FUN",
    discount: 3.33,
    index: 1,
    item_brand: "Google",
    item_category: "Apparel",
    item_category2: "Adult",
    item_category3: "Shirts",
    item_category4: "Crew",
    item_category5: "Short sleeve",
    item_list_id: "related_products",
    item_list_name: "Related Products",
    item_variant: "gray",
    location_id: "ChIJIQBpAG2ahYAR_6128GcTUEo",
    price: 21.01,
    promotion_id: "P_12345",
    promotion_name: "Summer Sale",
    google_business_vertical: "retail",
    custom_param: "custom_value",
    quantity: 2,
  };

  const pr = encodeItemToPrValue(item);
  assert.match(pr, /^idSKU_12346~nmGoogle Grey Women's Tee~/);
  assert.match(pr, /~k0google_business_vertical~v0retail~/);
  assert.match(pr, /~k1custom_param~v1custom_value(\$|$|~)/);
});

test("purchase vector: required params are produced from cookies + browser + event params", () => {
  const propertyId = "80e1d6d0-560d-419f-ac2a-fe9281e93386";
  const cookieHeader = [
    `_d8a=C1.1.457554353.1762098975`,
    `_d8a_${propertyId}=S1.1.s1767126418$o7$g1$t1767128356$j52$dabc`,
  ].join("; ");

  const browser = {
    dl: "https://example.test/",
    dt: "My Dev",
    dh: "example.test",
    dr: "",
    ul: "en-gb",
    sr: "5120x1440",
  };

  const eventParams = {
    transaction_id: "T_12345",
    value: 72.05,
    tax: 3.6,
    shipping: 5.99,
    currency: "USD",
    coupon: "SUMMER_SALE",
    customer_type: "new",
    items: [
      {
        item_id: "SKU_12345",
        item_name: "Stan and Friends Tee",
        affiliation: "Google Merchandise Store",
        coupon: "SUMMER_FUN",
        discount: 2.22,
        index: 0,
        item_brand: "Google",
        item_category: "Apparel",
        item_category2: "Adult",
        item_category3: "Shirts",
        item_category4: "Crew",
        item_category5: "Short sleeve",
        item_list_id: "related_products",
        item_list_name: "Related Products",
        item_variant: "green",
        location_id: "ChIJIQBpAG2ahYAR_6128GcTUEo",
        price: 10.01,
        google_business_vertical: "retail",
        quantity: 3,
      },
      {
        item_id: "SKU_12346",
        item_name: "Google Grey Women's Tee",
        affiliation: "Google Merchandise Store",
        coupon: "SUMMER_FUN",
        discount: 3.33,
        index: 1,
        item_brand: "Google",
        item_category: "Apparel",
        item_category2: "Adult",
        item_category3: "Shirts",
        item_category4: "Crew",
        item_category5: "Short sleeve",
        item_list_id: "related_products",
        item_list_name: "Related Products",
        item_variant: "gray",
        location_id: "ChIJIQBpAG2ahYAR_6128GcTUEo",
        price: 21.01,
        promotion_id: "P_12345",
        promotion_name: "Summer Sale",
        google_business_vertical: "retail",
        quantity: 2,
      },
    ],
  };

  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId,
    eventName: "purchase",
    eventParams,
    cookieHeader,
    browser,
    pageLoadMs: 1767201433889,
    hitCounter: 2,
    engagementTimeMs: 3,
    // Baseline consent: all granted
    consentParams: { gcs: "G111", gcd: "13r3r3r2r5l1" },
  });

  // Required outputs (as per requirements doc), except endpoint path differences.
  assert.equal(q.get("v"), "2");
  assert.equal(q.get("tid"), propertyId);
  assert.equal(q.get("cid"), "457554353.1762098975");
  assert.equal(q.get("sid"), "1767126418");
  assert.equal(q.get("sct"), "7");
  assert.equal(q.get("seg"), "1");
  assert.equal(q.get("en"), "purchase");

  assert.equal(q.get("_p"), "1767201433889");
  assert.equal(q.get("_s"), "2");
  assert.equal(q.get("_et"), "3");

  assert.equal(q.get("dl"), "https://example.test/");
  assert.equal(q.get("dt"), "My Dev");
  assert.equal(q.get("dr"), "");
  assert.equal(q.get("ul"), "en-gb");
  assert.equal(q.get("sr"), "5120x1440");

  assert.equal(q.get("gcs"), "G111");
  assert.equal(q.get("gcd"), "13r3r3r2r5l1");

  assert.equal(q.get("cu"), "USD");
  assert.equal(q.get("ep.transaction_id"), "T_12345");
  assert.equal(q.get("epn.value"), "72.05");
  assert.equal(q.get("epn.tax"), "3.6");
  assert.equal(q.get("epn.shipping"), "5.99");
  assert.equal(q.get("ep.coupon"), "SUMMER_SALE");
  assert.equal(q.get("ep.customer_type"), "new");

  assert.ok(q.get("pr1"));
  assert.ok(q.get("pr2"));
  assert.match(q.get("pr2")!, /^idSKU_12346~nmGoogle Grey Women's Tee~/);
});

test("buildGa4CollectQueryParams: handles missing cookies and browser context", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  assert.equal(q.get("v"), "2");
  assert.equal(q.get("tid"), "80e1d6d0-560d-419f-ac2a-fe9281e93386");
  assert.equal(q.get("en"), "page_view");
  // No cid when cookies are missing
  assert.equal(q.get("cid"), null);
  // sid: defaults to current timestamp when cookies are missing or consent denied
  const sid = q.get("sid");
  assert.ok(sid != null, "sid should be set to current timestamp when cookies are missing");
  assert.ok(/^\d+$/.test(sid!), "sid should be a numeric timestamp");
  // sct: defaults to 1 when cookies are missing or consent denied
  assert.equal(q.get("sct"), "1");
});

test("buildGa4CollectQueryParams: handles null and undefined values", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "custom_event",
    eventParams: {
      null_value: null,
      undefined_value: undefined,
      empty_string: "",
      zero: 0,
      false_bool: false,
      true_bool: true,
    },
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  // Null/undefined should be skipped
  assert.equal(q.get("ep.null_value"), null);
  assert.equal(q.get("ep.undefined_value"), null);
  // Empty string should be included
  assert.equal(q.get("ep.empty_string"), "");
  // Zero should be included
  assert.equal(q.get("epn.zero"), "0");
  // Booleans should convert
  assert.equal(q.get("ep.false_bool"), "0");
  assert.equal(q.get("ep.true_bool"), "1");
});

test("buildGa4CollectQueryParams: handles special characters and encoding", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "custom_event",
    eventParams: {
      special_chars: "test~value&key=value?query#hash",
      unicode: "测试 🎉 émoji",
      newlines: "line1\nline2\rline3",
      quotes: 'test "quoted" string',
    },
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  assert.equal(q.get("ep.special_chars"), "test~value&key=value?query#hash");
  assert.equal(q.get("ep.unicode"), "测试 🎉 émoji");
  assert.equal(q.get("ep.newlines"), "line1\nline2\rline3");
  assert.equal(q.get("ep.quotes"), 'test "quoted" string');
});

test("buildGa4CollectQueryParams: handles numeric edge cases", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "purchase",
    eventParams: {
      value: -10.5,
      tax: 0,
      shipping: 999999.99,
      string_number: "42.5",
      negative_string: "-5",
      invalid_string: "not-a-number",
    },
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  // Negative numbers should be included
  assert.equal(q.get("epn.value"), "-10.5");
  // Zero should be included
  assert.equal(q.get("epn.tax"), "0");
  // Large numbers
  assert.equal(q.get("epn.shipping"), "999999.99");
  // String numbers in custom params are treated as strings (ep), not numbers (epn)
  assert.equal(q.get("ep.string_number"), "42.5");
  assert.equal(q.get("ep.negative_string"), "-5");
  // Invalid string numbers are still included as strings
  assert.equal(q.get("ep.invalid_string"), "not-a-number");
});

test("encodeItemToPrValue: handles items with null/undefined values", () => {
  const item = {
    item_id: "SKU_123",
    item_name: null,
    price: undefined,
    quantity: 0,
    discount: "",
    custom_null: null,
    custom_undefined: undefined,
    custom_empty: "",
  };

  const pr = encodeItemToPrValue(item);
  assert.match(pr, /^idSKU_123~/);
  // null/undefined should be skipped
  assert.doesNotMatch(pr, /nm/);
  assert.doesNotMatch(pr, /pr/);
  // Zero should be included
  assert.match(pr, /qt0/);
  // Empty string should be included
  assert.match(pr, /ds/);
  // Custom null/undefined should be skipped
  assert.doesNotMatch(pr, /custom_null/);
  assert.doesNotMatch(pr, /custom_undefined/);
  // Custom empty string should be included
  assert.match(pr, /custom_empty/);
});

test("encodeItemToPrValue: handles items with non-scalar values (arrays, objects)", () => {
  const item = {
    item_id: "SKU_123",
    item_name: "Test",
    nested_object: { key: "value" },
    array_value: [1, 2, 3],
    null_value: null,
    valid_string: "ok",
  };

  const pr = encodeItemToPrValue(item);
  assert.match(pr, /^idSKU_123~nmTest~/);
  // Non-scalar values should be skipped
  assert.doesNotMatch(pr, /nested_object/);
  assert.doesNotMatch(pr, /array_value/);
  // Valid scalar should be included
  assert.match(pr, /valid_string/);
});

test("encodeItemToPrValue: handles items with special characters in values", () => {
  const item = {
    item_id: "SKU~test&value",
    item_name: "Product with ~special~ chars",
    price: 10.5,
    custom_param: "value~with~tildes",
  };

  const pr = encodeItemToPrValue(item);
  assert.match(pr, /^idSKU~test&value~/);
  assert.match(pr, /~nmProduct with ~special~ chars~/);
  assert.match(pr, /~pr10\.5~/);
  assert.match(pr, /custom_param/);
});

test("buildGa4CollectQueryParams: handles empty items array", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "purchase",
    eventParams: {
      items: [],
    },
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  assert.equal(q.get("pr1"), null);
});

test("buildGa4CollectQueryParams: handles items array with null/undefined items", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "purchase",
    eventParams: {
      items: [
        { item_id: "SKU_1", item_name: "Item 1" },
        null,
        undefined,
        { item_id: "SKU_2", item_name: "Item 2" },
      ],
    },
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  assert.ok(q.get("pr1"));
  assert.match(q.get("pr1")!, /idSKU_1/);
  // Null/undefined items should result in empty pr values (skipped)
  assert.equal(q.get("pr2"), null);
  assert.equal(q.get("pr3"), null);
  assert.ok(q.get("pr4"));
  assert.match(q.get("pr4")!, /idSKU_2/);
});

test("buildGa4CollectQueryParams: limits items to 200", () => {
  const items = Array.from({ length: 250 }, (_, i) => ({
    item_id: `SKU_${i}`,
    item_name: `Item ${i}`,
  }));

  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "purchase",
    eventParams: { items },
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  // Should have pr1 through pr200
  assert.ok(q.get("pr1"));
  assert.ok(q.get("pr200"));
  // Should not have pr201
  assert.equal(q.get("pr201"), null);
});

test("buildGa4CollectQueryParams: handles clientId override", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    cookieHeader: "_d8a=C1.1.457554353.1762098975",
    clientId: "override.123456",
    browser: getDefaultBuildParams().browser,
  });

  // clientId override should win over cookie
  assert.equal(q.get("cid"), "override.123456");
});

test("buildGa4CollectQueryParams: handles userId with whitespace", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    userId: "  user123  ",
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  // userId should be trimmed
  assert.equal(q.get("uid"), "user123");
});

test("buildGa4CollectQueryParams: skips empty userId", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    userId: "   ",
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  assert.equal(q.get("uid"), null);
});

test("buildGa4CollectQueryParams: handles debug mode", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    debugMode: true,
    cookieHeader: "",
    browser: getDefaultBuildParams().browser,
  });

  assert.equal(q.get("_dbg"), "1");
  assert.equal(q.get("ep.debug_mode"), "1");
});

test("buildGa4CollectQueryParams: handles ignore_referrer", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    ignoreReferrer: true,
    browser: {
      ...getDefaultBuildParams().browser,
      dr: "https://referrer.test/",
    },
    cookieHeader: "",
  });

  assert.equal(q.get("ir"), "1");
  assert.equal(q.get("dr"), "https://referrer.test/");
});

test("buildGa4CollectQueryParams: userId maps to uid", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    eventParams: {},
    cookieHeader: "",
    clientId: "1.2",
    userId: "user-123",
    browser: {
      dl: "https://example.test/",
      dt: "t",
      dh: "example.test",
      dr: "",
      ul: "en",
      sr: "1x1",
    },
    consentParams: { gcs: "G111", gcd: "13r3r3r2r5l1" },
  });

  assert.equal(q.get("uid"), "user-123");
});

test("buildGa4CollectQueryParams: campaign + content_group map to GA params", () => {
  const q = buildGa4CollectQueryParams({
    ...getDefaultBuildParams(),
    propertyId: "80e1d6d0-560d-419f-ac2a-fe9281e93386",
    eventName: "page_view",
    eventParams: {},
    cookieHeader: "",
    clientId: "1.2",
    browser: {
      dl: "https://example.test/",
      dt: "t",
      dh: "example.test",
      dr: "",
      ul: "en",
      sr: "1x1",
    },
    campaign: {
      campaign_id: "CID",
      campaign_source: "SRC",
      campaign_medium: "MED",
      campaign_name: "NAME",
      campaign_term: "TERM",
      campaign_content: "CONTENT",
    },
    contentGroup: "GROUP1",
  });

  assert.equal(q.get("ci"), "CID");
  assert.equal(q.get("cs"), "SRC");
  assert.equal(q.get("cm"), "MED");
  assert.equal(q.get("cn"), "NAME");
  assert.equal(q.get("ct"), "TERM");
  assert.equal(q.get("cc"), "CONTENT");
  assert.equal(q.get("ep.content_group"), "GROUP1");
});
