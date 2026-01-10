import { test, expect, Request } from "@playwright/test";

test.describe("GA4 Duplicator", () => {
  const GA4_ORIGIN = "https://www.google-analytics.com";
  const D8A_ORIGIN = "https://global.t.d8a.tech";
  const D8A_PATH = "/b3cfcbd5-3723-49e3-ad39-3737394bf0d8/g/collect";

  test.beforeEach(async ({ page }) => {
    // Intercept GA4 calls and fulfill them to avoid actual network requests
    await page.route("**/google-analytics.com/g/collect*", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: "{}" });
    });

    // Intercept D8A calls and fulfill them
    await page.route("**/global.t.d8a.tech/**", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: "{}" });
    });

    await page.goto("/test.html");
  });

  async function verifyDuplication(
    page: any,
    triggerAction: () => Promise<void>,
    method: "GET" | "POST",
  ) {
    const ga4RequestPromise = page.waitForRequest(
      (req: Request) => req.url().includes(GA4_ORIGIN) && req.method() === method,
    );
    const d8aRequestPromise = page.waitForRequest(
      (req: Request) => req.url().includes(D8A_ORIGIN) && req.method() === method,
    );

    await triggerAction();

    const [ga4Req, d8aReq] = await Promise.all([ga4RequestPromise, d8aRequestPromise]);

    const ga4Url = new URL(ga4Req.url());
    const d8aUrl = new URL(d8aReq.url());

    // Verify search params match
    expect(d8aUrl.search).toBe(ga4Url.search);

    // Verify D8A path is correct
    expect(d8aUrl.pathname).toBe(D8A_PATH);

    // Verify body matches for POST
    if (method === "POST") {
      expect(await d8aReq.postData()).toBe(await ga4Req.postData());
    }
  }

  test("should duplicate Fetch GET requests", async ({ page }) => {
    await verifyDuplication(page, () => page.click("text=Fetch GET"), "GET");
  });

  test("should duplicate Fetch POST requests", async ({ page }) => {
    await verifyDuplication(page, () => page.click("text=Fetch POST"), "POST");
  });

  test("should duplicate XHR GET requests", async ({ page }) => {
    await verifyDuplication(page, () => page.click("text=XHR GET"), "GET");
  });

  test("should duplicate XHR POST requests", async ({ page }) => {
    await verifyDuplication(page, () => page.click("text=XHR POST"), "POST");
  });

  test("should duplicate Beacon requests", async ({ page }) => {
    // sendBeacon is always POST
    await verifyDuplication(page, () => page.click("text=Beacon"), "POST");
  });

  test("should duplicate Script tag requests", async ({ page }) => {
    // Script tags are always GET
    await verifyDuplication(page, () => page.click("text=Script"), "GET");
  });
});
