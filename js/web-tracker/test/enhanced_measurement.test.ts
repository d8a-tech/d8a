import test from "node:test";
import assert from "node:assert/strict";

import { installD8a } from "../src/install.ts";
import { ensureArraySlot } from "../src/utils/window_slots.ts";
import { PROPERTY_ID, makeWindowMock, tick, getD8a } from "./test-utils.ts";

function makeAnchor({
  href,
  id,
  className,
  textContent,
}: {
  href: string;
  id?: string;
  className?: string;
  textContent?: string;
}) {
  return {
    tagName: "A",
    href,
    parentNode: null,
    id,
    className,
    textContent,
    getAttribute: (name: string) => {
      if (name === "href") return href;
      if (name === "id") return id || null;
      if (name === "class") return className || null;
      return null;
    },
  };
}

function makeWindow({ href = "http://example.test/" }: { href?: string } = {}) {
  const u = new URL(href);
  return makeWindowMock({
    location: { href: u.href, hostname: u.hostname, protocol: u.protocol, search: u.search },
    screen: { width: 100, height: 100 },
    document: {
      title: "T",
      referrer: "",
      cookie: "",
    },
    // Defer the callback to a microtask to match the original test behavior.
    setTimeout: (fn: () => void) => {
      const id = 1;
      Promise.resolve().then(() => fn());
      return id;
    },
  });
}

test("enhanced measurement: site search fires view_search_results on page load (default query params)", async () => {
  const w = makeWindow({ href: "http://example.test/?q=hello" });
  // Queue commands before the library installs.
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  dl.push(["js", new Date("2025-01-01T00:00:00Z")]);
  dl.push([
    "config",
    PROPERTY_ID,
    { server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}` },
  ]);

  const { consumer } = installD8a({ windowRef: w });
  assert.deepEqual(consumer.getState().propertyIds, [PROPERTY_ID]);

  await tick();

  // One auto page_view + one site search.
  assert.equal(w.fetchCalls.length, 2);
  const urls = w.fetchCalls.map((c) => new URL(c.url));
  const ens = urls.map((u2: URL) => u2.searchParams.get("en")).sort();
  assert.deepEqual(ens, ["page_view", "view_search_results"]);
  const ss = urls.find((u2: URL) => u2.searchParams.get("en") === "view_search_results")!;
  assert.equal(ss.searchParams.get("ep.search_term"), "hello");
});

test("enhanced measurement: outbound click emits click event with outbound=1", async () => {
  const w = makeWindow({ href: "http://example.test/" });
  installD8a({ windowRef: w });

  const d8a = getD8a(w);
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
  });

  const a = makeAnchor({
    href: "https://other.test/path",
    id: "outbound-link-id",
    className: "outbound-link primary",
    textContent: "Go to other",
  });
  const clickHandler = w.__docListeners.get("click")?.[0];
  assert.ok(clickHandler);
  clickHandler({ target: a });

  await tick();

  // auto page_view + outbound click
  assert.equal(w.fetchCalls.length, 2);
  const u = new URL(w.fetchCalls[1].url);
  assert.equal(u.searchParams.get("en"), "click");
  assert.equal(u.searchParams.get("ep.outbound"), "1");
  assert.equal(u.searchParams.get("ep.link_domain"), "other.test");
  assert.equal(u.searchParams.get("ep.link_id"), "outbound-link-id");
  assert.equal(u.searchParams.get("ep.link_classes"), "outbound-link primary");
});

test("enhanced measurement: file download emits file_download event", async () => {
  const w = makeWindow({ href: "http://example.test/" });
  installD8a({ windowRef: w });

  const d8a = getD8a(w);
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
  });

  const a = makeAnchor({
    href: "https://example.test/files/report.pdf",
    id: "download-link-id",
    className: "download-link",
    textContent: "Download report",
  });
  const clickHandler = w.__docListeners.get("click")?.[0];
  assert.ok(clickHandler);
  clickHandler({ target: a });

  await tick();

  // auto page_view + file_download
  assert.equal(w.fetchCalls.length, 2);
  const u = new URL(w.fetchCalls[1].url);
  assert.equal(u.searchParams.get("en"), "file_download");
  assert.equal(u.searchParams.get("ep.file_extension"), "pdf");
  assert.equal(u.searchParams.get("ep.file_name"), "report.pdf");
  assert.equal(u.searchParams.get("ep.link_id"), "download-link-id");
  assert.equal(u.searchParams.get("ep.link_classes"), "download-link");
  assert.equal(u.searchParams.get("ep.link_text"), "Download report");
});

test("enhanced measurement: outbound clicks can be disabled via config", async () => {
  const w = makeWindow({ href: "http://example.test/" });
  installD8a({ windowRef: w });

  const d8a = getD8a(w);
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    outbound_clicks_enabled: false,
  });

  const a = makeAnchor({ href: "https://other.test/path" });
  const clickHandler = w.__docListeners.get("click")?.[0];
  assert.ok(clickHandler);
  clickHandler({ target: a });

  await tick();

  // only auto page_view
  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("en"), "page_view");
});

test("enhanced measurement: file download on outbound link only fires file_download (not outbound click)", async () => {
  const w = makeWindow({ href: "http://example.test/" });
  installD8a({ windowRef: w });

  const d8a = getD8a(w);
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
  });

  // Link is both a download (PDF) and outbound (different domain)
  const a = makeAnchor({ href: "https://other.test/files/report.pdf" });
  const clickHandler = w.__docListeners.get("click")?.[0];
  assert.ok(clickHandler);
  clickHandler({ target: a });

  await tick();

  // auto page_view + file_download (but NOT outbound click)
  assert.equal(w.fetchCalls.length, 2);
  const u = new URL(w.fetchCalls[1].url);
  assert.equal(u.searchParams.get("en"), "file_download");
  assert.equal(u.searchParams.get("ep.file_extension"), "pdf");
  assert.equal(u.searchParams.get("ep.file_name"), "report.pdf");
  // Should NOT have outbound=1
  assert.equal(u.searchParams.get("ep.outbound"), null);
});

test("enhanced measurement: site search can be disabled via config", async () => {
  const w = makeWindow({ href: "http://example.test/?q=hello" });
  const dl = ensureArraySlot<unknown>(w, "d8aLayer");
  dl.push(["js", new Date("2025-01-01T00:00:00Z")]);
  dl.push([
    "config",
    PROPERTY_ID,
    {
      server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
      site_search_enabled: false,
    },
  ]);

  installD8a({ windowRef: w });

  await tick();

  // only auto page_view (no site search event)
  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("en"), "page_view");
});

test("enhanced measurement: file downloads can be disabled via config", async () => {
  const w = makeWindow({ href: "http://example.test/" });
  installD8a({ windowRef: w });

  const d8a = getD8a(w);
  d8a("config", PROPERTY_ID, {
    server_container_url: `https://tracker.example.test/d/c/${PROPERTY_ID}`,
    file_downloads_enabled: false,
  });

  const a = makeAnchor({ href: "https://example.test/files/report.pdf" });
  const clickHandler = w.__docListeners.get("click")?.[0];
  assert.ok(clickHandler);
  clickHandler({ target: a });

  await tick();

  // only auto page_view (no file_download event)
  assert.equal(w.fetchCalls.length, 1);
  const u = new URL(w.fetchCalls[0].url);
  assert.equal(u.searchParams.get("en"), "page_view");
});
