/**
 * GA4 Duplicator - Intercepts GA4 collect calls and sends duplicates to D8A.
 */

import { version } from "./version";

interface GA4Destination {
  measurement_id: string;
  server_container_url: string;
  convert_to_get?: boolean;
}

interface GA4DuplicatorOptions {
  server_container_url?: string;
  destinations?: GA4Destination[];
  debug?: boolean;
  convert_to_get?: boolean;
}

(window as any).createGA4Duplicator = function (options: GA4DuplicatorOptions) {
  /**
   * Shared logic and configuration for interceptors.
   */
  interface InterceptorContext {
    debug: boolean;
    isTargetUrl(url: string | null | undefined): boolean;
    buildDuplicateUrl(url: string): string;
    getConvertToGet(url: string): boolean;
  }

  /**
   * Base interface for monkey-patching implementations.
   */
  interface NetworkInterceptor {
    install(context: InterceptorContext): void;
  }

  class FetchInterceptor implements NetworkInterceptor {
    install(ctx: InterceptorContext): void {
      const originalFetch = window.fetch;
      window.fetch = function (
        this: any,
        resource: RequestInfo | URL,
        config?: RequestInit,
      ): Promise<Response> {
        const requestUrl =
          typeof resource === "string"
            ? resource
            : resource instanceof URL
              ? resource.toString()
              : (resource as any).url;
        const method = (config && config.method) || (resource as any).method || "GET";

        if (ctx.isTargetUrl(requestUrl)) {
          const upperMethod = (method || "GET").toUpperCase();

          // If we need the body from a Request, clone BEFORE calling the original fetch
          let prepareBodyPromise: Promise<any> = Promise.resolve(undefined);
          if (upperMethod === "POST") {
            if (config && Object.prototype.hasOwnProperty.call(config, "body")) {
              prepareBodyPromise = Promise.resolve(config.body);
            } else if (typeof Request !== "undefined" && resource instanceof Request) {
              try {
                const clonedReq = resource.clone();
                prepareBodyPromise = clonedReq.blob().catch(() => undefined);
              } catch {
                prepareBodyPromise = Promise.resolve(undefined);
              }
            }
          }

          // First send original request to Google Analytics
          const originalPromise = originalFetch.apply(this, arguments as any);

          // Then send duplicate
          const duplicateUrl = ctx.buildDuplicateUrl(requestUrl);
          const convertToGet = ctx.getConvertToGet(requestUrl);

          if (upperMethod === "GET") {
            originalFetch(duplicateUrl, { method: "GET", keepalive: true }).catch((error) => {
              if (ctx.debug) console.error("gtm interceptor: error duplicating GET fetch:", error);
            });
          } else if (upperMethod === "POST") {
            prepareBodyPromise.then((dupBody) => {
              if (convertToGet) {
                // Convert POST to GET: split body into lines and send each as a separate GET request
                let bodyStr = "";
                if (typeof dupBody === "string") {
                  bodyStr = dupBody;
                } else if (dupBody instanceof Blob) {
                  // For blob, we can't easily convert synchronously, so fall back to POST
                  originalFetch(duplicateUrl, {
                    method: "POST",
                    body: dupBody,
                    keepalive: true,
                  }).catch((error) => {
                    if (ctx.debug)
                      console.error(
                        "gtm interceptor: error duplicating POST fetch (convert_to_get with Blob):",
                        error,
                      );
                  });
                  return;
                }

                const lines = bodyStr.split("\n");
                let sentAny = false;
                for (let i = 0; i < lines.length; i++) {
                  const line = lines[i].trim();
                  if (line) {
                    const mergedUrl = ctx.buildDuplicateUrl(requestUrl);
                    const urlWithMergedLine = mergeBodyLineWithUrl(mergedUrl, line);
                    originalFetch(urlWithMergedLine, { method: "GET", keepalive: true }).catch(
                      (error) => {
                        if (ctx.debug)
                          console.error(
                            "gtm interceptor: error duplicating GET fetch (from convert_to_get):",
                            error,
                          );
                      },
                    );
                    sentAny = true;
                  }
                }

                // If body is empty or all lines were empty, send one GET with just URL params
                if (!sentAny) {
                  originalFetch(duplicateUrl, { method: "GET", keepalive: true }).catch((error) => {
                    if (ctx.debug)
                      console.error(
                        "gtm interceptor: error duplicating GET fetch (empty body convert_to_get):",
                        error,
                      );
                  });
                }
              } else {
                // Original POST duplication
                originalFetch(duplicateUrl, {
                  method: "POST",
                  body: dupBody,
                  keepalive: true,
                }).catch((error) => {
                  if (ctx.debug)
                    console.error("gtm interceptor: error duplicating POST fetch:", error);
                });
              }
            });
          }

          return originalPromise;
        }

        return originalFetch.apply(this, arguments as any);
      };
    }
  }

  class XhrInterceptor implements NetworkInterceptor {
    install(ctx: InterceptorContext): void {
      const originalXHROpen = XMLHttpRequest.prototype.open;
      const originalXHRSend = XMLHttpRequest.prototype.send;

      XMLHttpRequest.prototype.open = function (this: any, method: string, url: string | URL) {
        this._requestMethod = method;
        this._requestUrl = url;
        return originalXHROpen.apply(this, arguments as any);
      };

      XMLHttpRequest.prototype.send = function (
        this: any,
        body?: Document | XMLHttpRequestBodyInit | null,
      ) {
        if (this._requestUrl && ctx.isTargetUrl(this._requestUrl)) {
          // First send original request to Google Analytics
          const originalResult = originalXHRSend.apply(this, arguments as any);

          // Then send duplicate to our endpoint mimicking method and payload
          try {
            const method = (this._requestMethod || "GET").toUpperCase();
            const duplicateUrl = ctx.buildDuplicateUrl(this._requestUrl);
            const convertToGet = ctx.getConvertToGet(this._requestUrl);

            if (method === "GET") {
              fetch(duplicateUrl, { method: "GET", keepalive: true }).catch((error) => {
                if (ctx.debug) console.error("gtm interceptor: error duplicating GET xhr:", error);
              });
            } else if (method === "POST") {
              if (convertToGet) {
                // Convert POST to GET: split body into lines and send each as a separate GET request
                let bodyStr = "";
                if (typeof body === "string") {
                  bodyStr = body;
                } else if (body && typeof body === "object") {
                  // Try to convert Document or other types to string
                  try {
                    bodyStr = String(body);
                  } catch {
                    bodyStr = "";
                  }
                }

                const lines = bodyStr.split("\n");
                let sentAny = false;
                for (let i = 0; i < lines.length; i++) {
                  const line = lines[i].trim();
                  if (line) {
                    const mergedUrl = ctx.buildDuplicateUrl(this._requestUrl);
                    const urlWithMergedLine = mergeBodyLineWithUrl(mergedUrl, line);
                    fetch(urlWithMergedLine, { method: "GET", keepalive: true }).catch((error) => {
                      if (ctx.debug)
                        console.error(
                          "gtm interceptor: error duplicating GET xhr (from convert_to_get):",
                          error,
                        );
                    });
                    sentAny = true;
                  }
                }

                // If body is empty or all lines were empty, send one GET with just URL params
                if (!sentAny) {
                  fetch(duplicateUrl, { method: "GET", keepalive: true }).catch((error) => {
                    if (ctx.debug)
                      console.error(
                        "gtm interceptor: error duplicating GET xhr (empty body convert_to_get):",
                        error,
                      );
                  });
                }
              } else {
                // Original POST duplication
                fetch(duplicateUrl, { method: "POST", body: body as any, keepalive: true }).catch(
                  (error) => {
                    if (ctx.debug)
                      console.error("gtm interceptor: error duplicating POST xhr:", error);
                  },
                );
              }
            }
          } catch (dupErr) {
            if (ctx.debug) console.error("gtm interceptor: xhr duplication failed:", dupErr);
          }
          return originalResult;
        }
        return originalXHRSend.apply(this, arguments as any);
      };
    }
  }

  class BeaconInterceptor implements NetworkInterceptor {
    install(ctx: InterceptorContext): void {
      if (!navigator.sendBeacon) return;

      const originalSendBeacon = navigator.sendBeacon;
      navigator.sendBeacon = function (
        this: any,
        url: string | URL,
        data?: BodyInit | null,
      ): boolean {
        if (ctx.isTargetUrl(url as string)) {
          const originalResult = originalSendBeacon.apply(this, arguments as any);
          try {
            const duplicateUrl = ctx.buildDuplicateUrl(url as string);
            const convertToGet = ctx.getConvertToGet(url as string);

            if (convertToGet) {
              // Convert sendBeacon POST to GET: split body into lines and send each as a separate GET request
              let bodyStr = "";
              if (typeof data === "string") {
                bodyStr = data;
              } else if (data && typeof data === "object") {
                try {
                  bodyStr = String(data);
                } catch {
                  bodyStr = "";
                }
              }

              const lines = bodyStr.split("\n");
              let sentAny = false;
              for (let i = 0; i < lines.length; i++) {
                const line = lines[i].trim();
                if (line) {
                  const mergedUrl = ctx.buildDuplicateUrl(url as string);
                  const urlWithMergedLine = mergeBodyLineWithUrl(mergedUrl, line);
                  fetch(urlWithMergedLine, { method: "GET", keepalive: true }).catch((error) => {
                    if (ctx.debug)
                      console.error(
                        "gtm interceptor: error duplicating GET beacon (from convert_to_get):",
                        error,
                      );
                  });
                  sentAny = true;
                }
              }

              // If body is empty or all lines were empty, send one GET with just URL params
              if (!sentAny) {
                fetch(duplicateUrl, { method: "GET", keepalive: true }).catch((error) => {
                  if (ctx.debug)
                    console.error(
                      "gtm interceptor: error duplicating GET beacon (empty body convert_to_get):",
                      error,
                    );
                });
              }
            } else {
              // Original sendBeacon duplication
              originalSendBeacon.call(navigator, duplicateUrl, data);
            }
          } catch (e) {
            if (ctx.debug) console.error("gtm interceptor: error duplicating sendBeacon:", e);
          }
          return originalResult;
        }
        return originalSendBeacon.apply(this, arguments as any);
      };
    }
  }

  class ScriptInterceptor implements NetworkInterceptor {
    install(ctx: InterceptorContext): void {
      try {
        const scriptSrcDescriptor = Object.getOwnPropertyDescriptor(
          HTMLScriptElement.prototype,
          "src",
        );
        const originalScriptSrcSetter = scriptSrcDescriptor && scriptSrcDescriptor.set;
        const originalScriptSrcGetter = scriptSrcDescriptor && scriptSrcDescriptor.get;
        const originalScriptSetAttribute = HTMLScriptElement.prototype.setAttribute;

        const duplicateIfGA4Url = (urlString: string) => {
          try {
            if (!ctx.isTargetUrl(urlString)) return;
            fetch(ctx.buildDuplicateUrl(urlString), { method: "GET", keepalive: true }).catch(
              (error) => {
                if (ctx.debug)
                  console.error("gtm interceptor: error duplicating script GET:", error);
              },
            );
          } catch {
            // Intentionally empty
          }
        };

        if (originalScriptSrcSetter && originalScriptSrcGetter) {
          const setter = originalScriptSrcSetter;
          const getter = originalScriptSrcGetter;
          Object.defineProperty(HTMLScriptElement.prototype, "src", {
            configurable: true,
            enumerable: true,
            get: function (this: any) {
              return getter.call(this);
            },
            set: function (this: any, value: string) {
              try {
                const last = this.__ga4LastSrcDuplicated;
                if (value && value !== last) {
                  duplicateIfGA4Url(String(value));
                  this.__ga4LastSrcDuplicated = String(value);
                }
                const self = this;
                const onloadOnce = function () {
                  try {
                    const finalUrl = self.src;
                    if (finalUrl && finalUrl !== self.__ga4LastSrcDuplicated) {
                      duplicateIfGA4Url(finalUrl);
                      self.__ga4LastSrcDuplicated = finalUrl;
                    }
                  } catch {}
                  self.removeEventListener("load", onloadOnce);
                };
                this.addEventListener("load", onloadOnce);
              } catch {}
              setter.call(this, value);
            },
          });
        }

        HTMLScriptElement.prototype.setAttribute = function (
          this: any,
          name: string,
          value: string,
        ) {
          try {
            if (String(name).toLowerCase() === "src") {
              const v = String(value);
              const last = this.__ga4LastSrcDuplicated;
              if (v && v !== last) {
                duplicateIfGA4Url(v);
                this.__ga4LastSrcDuplicated = v;
              }
              const selfAttr = this;
              const onloadOnceAttr = function () {
                try {
                  const finalUrlAttr = selfAttr.src;
                  if (finalUrlAttr && finalUrlAttr !== selfAttr.__ga4LastSrcDuplicated) {
                    duplicateIfGA4Url(finalUrlAttr);
                    selfAttr.__ga4LastSrcDuplicated = finalUrlAttr;
                  }
                } catch {}
                selfAttr.removeEventListener("load", onloadOnceAttr);
              };
              this.addEventListener("load", onloadOnceAttr);
            }
          } catch {
            // Intentionally empty
          }
          return originalScriptSetAttribute.apply(this, arguments as any);
        };
      } catch {}
    }
  }

  if ((window as any).__ga4DuplicatorInitialized) {
    if (options.debug) console.warn("GA4 Duplicator: already initialized.");
    return;
  }

  const destinations: GA4Destination[] = [];
  if (options.destinations && Array.isArray(options.destinations)) {
    for (let i = 0; i < options.destinations.length; i++) {
      const dest = options.destinations[i];
      destinations.push({
        measurement_id: dest.measurement_id,
        server_container_url: dest.server_container_url,
        convert_to_get:
          dest.convert_to_get !== undefined ? dest.convert_to_get : options.convert_to_get,
      });
    }
  }

  if (options.server_container_url) {
    destinations.push({
      measurement_id: "*",
      server_container_url: options.server_container_url,
      convert_to_get: options.convert_to_get,
    });
  }

  if (destinations.length === 0) {
    console.error("GA4 Duplicator: either server_container_url or destinations array is required");
    return;
  }

  function normalizePath(p: string): string {
    p = String(p || "");
    p = p.replace(/\/+$/, "");
    return p === "" ? "/" : p;
  }

  function matchesId(pattern: string, id: string): boolean {
    if (!pattern || pattern === "*") return true;
    try {
      const regexStr = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*");
      return new RegExp("^" + regexStr + "$", "i").test(id);
    } catch {
      return pattern.toLowerCase() === id.toLowerCase();
    }
  }

  function ensureBareQueryFlag(url: string, flag: string): string {
    const u = String(url || "");
    const f = String(flag || "").trim();
    if (!u || !f) return u;

    // Keep fragment intact
    const hashIdx = u.indexOf("#");
    const beforeHash = hashIdx >= 0 ? u.slice(0, hashIdx) : u;
    const hash = hashIdx >= 0 ? u.slice(hashIdx) : "";

    const qIdx = beforeHash.indexOf("?");
    const base = qIdx >= 0 ? beforeHash.slice(0, qIdx) : beforeHash;
    const rawQuery = qIdx >= 0 ? beforeHash.slice(qIdx + 1) : "";

    const parts = rawQuery
      ? rawQuery
          .split("&")
          .map((p) => p.trim())
          .filter(Boolean)
      : [];

    const kept: string[] = [];
    for (let i = 0; i < parts.length; i++) {
      const p = parts[i];
      if (p === f) continue;
      if (p.startsWith(f + "=")) continue;
      kept.push(p);
    }

    const newQuery = kept.length > 0 ? kept.join("&") + "&" + f : f;
    return base + "?" + newQuery + hash;
  }

  function getMeasurementId(url: string): string {
    try {
      const parsed = new URL(url, location.href);
      return parsed.searchParams.get("tid") || parsed.searchParams.get("id") || "";
    } catch {
      const match = url.match(/[?&](?:tid|id)=([^&?#]+)/);
      return match ? decodeURIComponent(match[1]) : "";
    }
  }

  function getDestinationForId(id: string): GA4Destination | null {
    for (let i = 0; i < destinations.length; i++) {
      if (matchesId(destinations[i].measurement_id, id)) {
        return destinations[i];
      }
    }
    return null;
  }

  function mergeBodyLineWithUrl(originalUrl: string, bodyLine: string): string {
    try {
      const url = new URL(originalUrl, location.href);
      const lineParams = new URLSearchParams(bodyLine);

      // Line parameters override URL parameters
      for (const [key] of lineParams.entries()) {
        url.searchParams.delete(key);
      }
      for (const [key, value] of lineParams.entries()) {
        url.searchParams.append(key, value);
      }

      return ensureBareQueryFlag(url.toString(), "richsstsse");
    } catch {
      // Fallback: if URL parsing fails, try string manipulation
      const urlWithoutQuery = originalUrl.split("?")[0];
      const originalParams = originalUrl.match(/\?(.*)/) ? originalUrl.match(/\?(.*)/)![1] : "";
      const merged = originalParams + (originalParams && bodyLine ? "&" : "") + bodyLine;
      return ensureBareQueryFlag(urlWithoutQuery + (merged ? "?" + merged : ""), "richsstsse");
    }
  }

  function getDuplicateEndpointUrl(dest: GA4Destination): URL {
    const trackingURL = String(dest.server_container_url || "").trim();
    const u = new URL(trackingURL, location.href);
    u.search = "";
    u.hash = "";
    return u;
  }

  function isTargetUrl(url: string | null | undefined): boolean {
    if (!url || typeof url !== "string") return false;
    try {
      const parsed = new URL(url, location.href);

      for (let i = 0; i < destinations.length; i++) {
        const duplicateTarget = getDuplicateEndpointUrl(destinations[i]);
        if (
          parsed.origin === duplicateTarget.origin &&
          normalizePath(parsed.pathname) === normalizePath(duplicateTarget.pathname)
        ) {
          return false;
        }
      }

      const params = parsed.searchParams;
      const hasGtm = params.has("gtm");
      const hasTagExp = params.has("tag_exp");
      const measurementId = params.get("tid") || params.get("id") || "";
      const isMeasurementIdGA4 = /^G-[A-Z0-9]+$/i.test(measurementId);

      return hasGtm && hasTagExp && isMeasurementIdGA4;
    } catch {
      if (typeof url === "string") {
        for (let j = 0; j < destinations.length; j++) {
          try {
            const target = getDuplicateEndpointUrl(destinations[j]);
            const targetNoQuery = target.origin + target.pathname;
            if (url.indexOf(targetNoQuery) !== -1) return false;
          } catch {
            // Intentionally empty
          }
        }

        const hasGtmFallback = url.indexOf("gtm=") !== -1;
        const hasTagExpFallback = url.indexOf("tag_exp=") !== -1;
        const idMatch = url.match(/[?&](?:tid|id)=G-[A-Za-z0-9]+/);
        return !!(hasGtmFallback && hasTagExpFallback && idMatch);
      }
      return false;
    }
  }

  function buildDuplicateUrl(originalUrl: string): string {
    const id = getMeasurementId(originalUrl);
    const dest = getDestinationForId(id);
    if (!dest) return "";

    const dst = getDuplicateEndpointUrl(dest);
    try {
      const src = new URL(originalUrl, location.href);
      dst.search = src.search;
      dst.searchParams.set("_dtv", version);
      dst.searchParams.set("_dtn", "gd");
    } catch {}
    return ensureBareQueryFlag(dst.toString(), "richsstsse");
  }

  function getConvertToGet(url: string): boolean {
    const id = getMeasurementId(url);
    const dest = getDestinationForId(id);
    return dest ? !!dest.convert_to_get : false;
  }

  const context: InterceptorContext = {
    debug: !!options.debug,
    isTargetUrl,
    buildDuplicateUrl,
    getConvertToGet,
  };

  const interceptors: NetworkInterceptor[] = [
    new FetchInterceptor(),
    new XhrInterceptor(),
    new BeaconInterceptor(),
    new ScriptInterceptor(),
  ];

  for (let i = 0; i < interceptors.length; i++) {
    try {
      interceptors[i].install(context);
    } catch (e) {
      if (options.debug) console.error("GA4 Duplicator: failed to install interceptor", e);
    }
  }

  (window as any).__ga4DuplicatorInitialized = true;
};
