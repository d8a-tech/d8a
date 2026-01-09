// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import "./ga4-duplicator";

describe("GA4 Duplicator Blackbox Tests", () => {
  const SERVER_URL = "https://my-d8a-server.com/collect";
  const GA4_URL_BASE = "https://www.google-analytics.com/g/collect";
  const GA4_QUERY = "?v=2&tid=G-12345&gtm=297&tag_exp=1";
  const GA4_URL = `${GA4_URL_BASE}${GA4_QUERY}`;

  let fetchMock: any;
  let sendBeaconMock: any;
  let xhrSendMock: any;
  let xhrOpenMock: any;

  // Capture original prototypes once before any tests run
  const originals = {
    xhrOpen: XMLHttpRequest.prototype.open,
    xhrSend: XMLHttpRequest.prototype.send,
    scriptSrcDescriptor: Object.getOwnPropertyDescriptor(HTMLScriptElement.prototype, "src"),
    scriptSetAttribute: HTMLScriptElement.prototype.setAttribute,
  };

  // Check if navigator.sendBeacon exists (it might not in all jsdom versions)
  const originalSendBeacon = navigator.sendBeacon;

  beforeEach(() => {
    // Reset initialization flag so we can re-initialize
    delete (window as any).__ga4DuplicatorInitialized;

    // 1. Mock fetch (used by FetchInterceptor and as duplicate sender for XHR/Script)
    fetchMock = vi.fn().mockResolvedValue(new Response("ok"));
    vi.stubGlobal("fetch", fetchMock);

    // 2. Mock sendBeacon (used by BeaconInterceptor)
    sendBeaconMock = vi.fn().mockReturnValue(true);
    // We use defineProperty because navigator might be read-only or getter in some envs
    Object.defineProperty(navigator, "sendBeacon", {
      value: sendBeaconMock,
      writable: true,
      configurable: true,
    });

    // 3. Mock XHR internals to prevent real network calls and allow tracking
    // These mocks will be the "original" methods wrapped by the duplicator
    xhrSendMock = vi.fn();
    xhrOpenMock = vi.fn();
    XMLHttpRequest.prototype.send = xhrSendMock;
    XMLHttpRequest.prototype.open = xhrOpenMock;
  });

  afterEach(() => {
    vi.unstubAllGlobals();

    // Restore prototypes to original jsdom implementations
    XMLHttpRequest.prototype.open = originals.xhrOpen;
    XMLHttpRequest.prototype.send = originals.xhrSend;

    if (originals.scriptSrcDescriptor) {
      Object.defineProperty(HTMLScriptElement.prototype, "src", originals.scriptSrcDescriptor);
    }
    HTMLScriptElement.prototype.setAttribute = originals.scriptSetAttribute;

    if (originalSendBeacon) {
      Object.defineProperty(navigator, "sendBeacon", {
        value: originalSendBeacon,
        writable: true,
        configurable: true,
      });
    } else {
      // If it didn't exist, we could delete it, but leaving it undefined/mocked might be fine or:
      // delete (navigator as any).sendBeacon;
    }

    document.body.innerHTML = "";
    vi.clearAllMocks();
  });

  const initDuplicator = (options: any = {}) => {
    (window as any).createGA4Duplicator({
      server_container_url: SERVER_URL,
      debug: true,
      ...options,
    });
  };

  const verifyDuplicateSent = (
    mockFn: any,
    callIndex: number = 0,
    expectedUrlPart: string = SERVER_URL,
    expectedTid: string = "G-12345",
  ) => {
    expect(mockFn).toHaveBeenCalled();
    const calls = mockFn.mock.calls;
    expect(calls.length).toBeGreaterThan(callIndex);

    const url = calls[callIndex][0];
    expect(url).toContain(expectedUrlPart);
    if (expectedTid) {
      expect(url).toContain(`tid=${expectedTid}`);
    }
    return calls[callIndex];
  };

  describe("Fetch Interception", () => {
    it("should duplicate a GA4 fetch request", async () => {
      initDuplicator();
      await fetch(GA4_URL, { method: "GET" });

      // 1. Original fetch called
      // 2. Duplicate fetch called
      expect(fetchMock).toHaveBeenCalledTimes(2);

      const firstCallUrl = fetchMock.mock.calls[0][0];
      expect(firstCallUrl).toBe(GA4_URL);

      verifyDuplicateSent(fetchMock, 1);
    });

    it("should NOT duplicate non-GA4 fetch request", async () => {
      initDuplicator();
      const OTHER_URL = "https://example.com/api";
      await fetch(OTHER_URL, { method: "GET" });

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(fetchMock.mock.calls[0][0]).toBe(OTHER_URL);
    });

    it("should route to different destinations based on tid", async () => {
      const DEST1_URL = "https://dest1.com";
      const DEST2_URL = "https://dest2.com";
      const DEFAULT_URL = "https://default.com";

      initDuplicator({
        destinations: [
          { measurement_id: "G-SPECIFIC1", server_container_url: DEST1_URL },
          { measurement_id: "G-SPECIFIC2", server_container_url: DEST2_URL },
        ],
        server_container_url: DEFAULT_URL,
      });

      const URL1 = "https://www.google-analytics.com/g/collect?v=2&tid=G-SPECIFIC1&gtm=1&tag_exp=1";
      const URL2 = "https://www.google-analytics.com/g/collect?v=2&tid=G-SPECIFIC2&gtm=1&tag_exp=1";
      const URL_OTHER =
        "https://www.google-analytics.com/g/collect?v=2&tid=G-OTHER&gtm=1&tag_exp=1";

      await fetch(URL1, { method: "GET" });
      await fetch(URL2, { method: "GET" });
      await fetch(URL_OTHER, { method: "GET" });

      // Total 6 calls: 3 original + 3 duplicates
      expect(fetchMock).toHaveBeenCalledTimes(6);

      // Check duplicates (interleaved with original calls, so indices 1, 3, 5)
      verifyDuplicateSent(fetchMock, 1, DEST1_URL, "G-SPECIFIC1");
      verifyDuplicateSent(fetchMock, 3, DEST2_URL, "G-SPECIFIC2");
      verifyDuplicateSent(fetchMock, 5, DEFAULT_URL, "G-OTHER");
    });

    it("should not double-append /g/collect when server_container_url is origin-only (regression)", async () => {
      // given
      const ORIGIN_ONLY = "https://global.t.d8a.tech";
      initDuplicator({ server_container_url: ORIGIN_ONLY });
    
      // when
      await fetch(GA4_URL, { method: "GET" });
    
      // then
      expect(fetchMock).toHaveBeenCalledTimes(2);
      const duplicateUrl = fetchMock.mock.calls[1][0] as string;
    
      // This pins the current suspected bug:
      expect(duplicateUrl).toContain("global.t.d8a.tech/g/collect");
    });

  });

  describe("XHR Interception", () => {
    it("should duplicate a GA4 XHR request", () => {
      initDuplicator();

      const xhr = new XMLHttpRequest();
      xhr.open("GET", GA4_URL);
      xhr.send();

      // Original XHR logic
      expect(xhrOpenMock).toHaveBeenCalledWith("GET", GA4_URL);
      expect(xhrSendMock).toHaveBeenCalled();

      // Duplicate logic (uses fetch)
      expect(fetchMock).toHaveBeenCalledTimes(1);
      verifyDuplicateSent(fetchMock, 0);
    });

    it("should handle POST XHR with body", () => {
      initDuplicator();
      const body = JSON.stringify({ event: "test" });

      const xhr = new XMLHttpRequest();
      xhr.open("POST", GA4_URL);
      xhr.send(body);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      const [, config] = verifyDuplicateSent(fetchMock, 0);

      expect(config.method).toBe("POST");
      expect(config.body).toBe(body);
    });

    it("should NOT duplicate non-GA4 XHR request", () => {
      initDuplicator();
      const OTHER_URL = "https://example.com/api";

      const xhr = new XMLHttpRequest();
      xhr.open("GET", OTHER_URL);
      xhr.send();

      expect(xhrSendMock).toHaveBeenCalled();
      expect(fetchMock).not.toHaveBeenCalled();
    });

    it("should route XHR to different destinations based on tid", () => {
      const DEST1_URL = "https://dest1.com";
      const DEFAULT_URL = "https://default.com";

      initDuplicator({
        destinations: [{ measurement_id: "G-SPECIFIC", server_container_url: DEST1_URL }],
        server_container_url: DEFAULT_URL,
      });

      const URL1 = "https://www.google-analytics.com/g/collect?v=2&tid=G-SPECIFIC&gtm=1&tag_exp=1";
      const URL_OTHER =
        "https://www.google-analytics.com/g/collect?v=2&tid=G-OTHER&gtm=1&tag_exp=1";

      const xhr1 = new XMLHttpRequest();
      xhr1.open("GET", URL1);
      xhr1.send();

      const xhr2 = new XMLHttpRequest();
      xhr2.open("GET", URL_OTHER);
      xhr2.send();

      expect(fetchMock).toHaveBeenCalledTimes(2);
      verifyDuplicateSent(fetchMock, 0, DEST1_URL, "G-SPECIFIC");
      verifyDuplicateSent(fetchMock, 1, DEFAULT_URL, "G-OTHER");
    });
  });

  describe("Beacon Interception", () => {
    it("should duplicate a GA4 sendBeacon request", () => {
      initDuplicator();
      const data = "beacon-data";

      navigator.sendBeacon(GA4_URL, data);

      // Beacon interceptor calls originalSendBeacon for duplicate too
      expect(sendBeaconMock).toHaveBeenCalledTimes(2);

      const originalCall = sendBeaconMock.mock.calls[0];
      expect(originalCall[0]).toBe(GA4_URL);

      const duplicateCall = verifyDuplicateSent(sendBeaconMock, 1);
      expect(duplicateCall[1]).toBe(data);
    });

    it("should NOT duplicate non-GA4 sendBeacon request", () => {
      initDuplicator();
      navigator.sendBeacon("https://other.com", "data");
      expect(sendBeaconMock).toHaveBeenCalledTimes(1);
    });

    it("should route sendBeacon to different destinations based on tid", () => {
      const DEST1_URL = "https://dest1.com";
      const DEFAULT_URL = "https://default.com";

      initDuplicator({
        destinations: [{ measurement_id: "G-SPECIFIC", server_container_url: DEST1_URL }],
        server_container_url: DEFAULT_URL,
      });

      const URL1 = "https://www.google-analytics.com/g/collect?v=2&tid=G-SPECIFIC&gtm=1&tag_exp=1";
      const URL_OTHER =
        "https://www.google-analytics.com/g/collect?v=2&tid=G-OTHER&gtm=1&tag_exp=1";

      navigator.sendBeacon(URL1, "data1");
      navigator.sendBeacon(URL_OTHER, "data2");

      // 2 original + 2 duplicates
      expect(sendBeaconMock).toHaveBeenCalledTimes(4);

      // Duplicates are second and fourth calls
      verifyDuplicateSent(sendBeaconMock, 1, DEST1_URL, "G-SPECIFIC");
      verifyDuplicateSent(sendBeaconMock, 3, DEFAULT_URL, "G-OTHER");
    });
  });

  describe("Script Tag Interception", () => {
    it("should duplicate when script src is set to GA4 URL via property", () => {
      initDuplicator();

      const script = document.createElement("script");
      script.src = GA4_URL;
      document.body.appendChild(script);

      // Script interceptor uses fetch for duplication
      expect(fetchMock).toHaveBeenCalledTimes(1);
      verifyDuplicateSent(fetchMock, 0);
    });

    it("should duplicate when script src is set via setAttribute", () => {
      initDuplicator();

      const script = document.createElement("script");
      script.setAttribute("src", GA4_URL);
      document.body.appendChild(script);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      verifyDuplicateSent(fetchMock, 0);
    });

    it("should NOT duplicate non-GA4 script", () => {
      initDuplicator();

      const script = document.createElement("script");
      script.src = "https://example.com/script.js";
      document.body.appendChild(script);

      expect(fetchMock).not.toHaveBeenCalled();
    });

    it("should route Script tags to different destinations based on tid", () => {
      const DEST1_URL = "https://dest1.com";
      const DEFAULT_URL = "https://default.com";

      initDuplicator({
        destinations: [{ measurement_id: "G-SPECIFIC", server_container_url: DEST1_URL }],
        server_container_url: DEFAULT_URL,
      });

      const URL1 = "https://www.google-analytics.com/g/collect?v=2&tid=G-SPECIFIC&gtm=1&tag_exp=1";
      const URL_OTHER =
        "https://www.google-analytics.com/g/collect?v=2&tid=G-OTHER&gtm=1&tag_exp=1";

      const s1 = document.createElement("script");
      s1.src = URL1;
      document.body.appendChild(s1);

      const s2 = document.createElement("script");
      s2.src = URL_OTHER;
      document.body.appendChild(s2);

      expect(fetchMock).toHaveBeenCalledTimes(2);
      verifyDuplicateSent(fetchMock, 0, DEST1_URL, "G-SPECIFIC");
      verifyDuplicateSent(fetchMock, 1, DEFAULT_URL, "G-OTHER");
    });
  });
});
