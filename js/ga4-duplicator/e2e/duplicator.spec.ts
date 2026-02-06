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

  async function initDuplicator(page: any) {
    await page.click("text=Initialize");
  }

  async function injectDuplicatorScript(page: any, count: number) {
    await page.evaluate(
      ({ count: c }) => {
        return new Promise<void>((resolve, reject) => {
          let loaded = 0;
          const base = "/dist/gd.js";
          const stamp = `${Date.now()}-${Math.random()}`;

          for (let i = 0; i < c; i++) {
            const s = document.createElement("script");
            s.async = false;
            s.src = `${base}?dup=${encodeURIComponent(stamp)}&i=${i}`;
            s.onload = () => {
              loaded++;
              if (loaded === c) resolve();
            };
            s.onerror = () => reject(new Error(`failed to load ${s.src}`));
            document.head.appendChild(s);
          }
        });
      },
      { count },
    );
  }

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

    // Verify all GA4 search params are preserved on the duplicated request.
    // The duplicator is expected to add its own params (_dtv/_dtn) on top.
    for (const [k, v] of ga4Url.searchParams.entries()) {
      expect(d8aUrl.searchParams.get(k)).toBe(v);
    }

    // Verify the duplicator markers are present
    expect(d8aUrl.searchParams.get("_dtv")).toBeTruthy();
    expect(d8aUrl.searchParams.get("_dtn")).toBe("gd");

    // Verify D8A path is correct
    expect(d8aUrl.pathname).toBe(D8A_PATH);

    // Verify body matches for POST
    if (method === "POST") {
      expect(await d8aReq.postData()).toBe(await ga4Req.postData());
    }
  }

  test("should duplicate Fetch GET requests", async ({ page }) => {
    await initDuplicator(page);
    await verifyDuplication(page, () => page.click("text=Fetch GET"), "GET");
  });

  test("should duplicate Fetch POST requests", async ({ page }) => {
    await initDuplicator(page);
    await verifyDuplication(page, () => page.click("text=Fetch POST"), "POST");
  });

  test("should duplicate XHR GET requests", async ({ page }) => {
    await initDuplicator(page);
    await verifyDuplication(page, () => page.click("text=XHR GET"), "GET");
  });

  test("should duplicate XHR POST requests", async ({ page }) => {
    await initDuplicator(page);
    await verifyDuplication(page, () => page.click("text=XHR POST"), "POST");
  });

  test("should duplicate Beacon requests", async ({ page }) => {
    await initDuplicator(page);
    // sendBeacon is always POST
    await verifyDuplication(page, () => page.click("text=Beacon"), "POST");
  });

  test("should duplicate Script tag requests", async ({ page }) => {
    await initDuplicator(page);
    // Script tags are always GET
    await verifyDuplication(page, () => page.click("text=Script"), "GET");
  });

  test("should be idempotent when initialized multiple times", async ({ page }) => {
    // given
    // Load the duplicator script multiple times (simulates repeated <script> inclusion).
    await injectDuplicatorScript(page, 5);
    // Give the browser a moment to execute the scripts.
    await page.waitForTimeout(250);

    // Initialize multiple times (simulates repeated init snippets).
    await initDuplicator(page);
    await initDuplicator(page);
    await initDuplicator(page);
    await initDuplicator(page);
    await initDuplicator(page);

    const ga4Requests: Request[] = [];
    const d8aRequests: Request[] = [];

    page.on("request", (req: Request) => {
      const url = req.url();
      if (url.includes(`${GA4_ORIGIN}/g/collect`)) ga4Requests.push(req);
      if (url.includes(D8A_ORIGIN)) d8aRequests.push(req);
    });

    const ga4RequestPromise = page.waitForRequest(
      (req: Request) => req.url().includes(`${GA4_ORIGIN}/g/collect`) && req.method() === "GET",
    );
    const d8aRequestPromise = page.waitForRequest(
      (req: Request) => req.url().includes(D8A_ORIGIN) && req.method() === "GET",
    );

    // when
    await page.click("text=Fetch GET");
    await Promise.all([ga4RequestPromise, d8aRequestPromise]);

    // then
    // Give the page a moment to emit any accidental extra duplicates.
    await page.waitForTimeout(250);

    expect(ga4Requests.length).toBe(1);
    expect(d8aRequests.length).toBe(1);

    const d8aUrl = new URL(d8aRequests[0].url());
    expect(d8aUrl.pathname).toBe(D8A_PATH);
  });
});
