/* ga4-duplicator - built 2026-01-21T13:52:07.734Z */
"use strict";
(() => {
  // src/ga4-duplicator.ts
  window.createGA4Duplicator = function(options) {
    class FetchInterceptor {
      install(ctx) {
        const originalFetch = window.fetch;
        window.fetch = function(resource, config) {
          const requestUrl = typeof resource === "string" ? resource : resource instanceof URL ? resource.toString() : resource.url;
          const method = config && config.method || resource.method || "GET";
          if (ctx.isTargetUrl(requestUrl)) {
            const upperMethod = (method || "GET").toUpperCase();
            let prepareBodyPromise = Promise.resolve(void 0);
            if (upperMethod === "POST") {
              if (config && Object.prototype.hasOwnProperty.call(config, "body")) {
                prepareBodyPromise = Promise.resolve(config.body);
              } else if (typeof Request !== "undefined" && resource instanceof Request) {
                try {
                  const clonedReq = resource.clone();
                  prepareBodyPromise = clonedReq.blob().catch(() => void 0);
                } catch (e) {
                  prepareBodyPromise = Promise.resolve(void 0);
                }
              }
            }
            const originalPromise = originalFetch.apply(this, arguments);
            const duplicateUrl = ctx.buildDuplicateUrl(requestUrl);
            if (upperMethod === "GET") {
              originalFetch(duplicateUrl, { method: "GET", keepalive: true }).catch((error) => {
                if (ctx.debug) console.error("gtm interceptor: error duplicating GET fetch:", error);
              });
            } else if (upperMethod === "POST") {
              prepareBodyPromise.then((dupBody) => {
                originalFetch(duplicateUrl, { method: "POST", body: dupBody, keepalive: true }).catch(
                  (error) => {
                    if (ctx.debug)
                      console.error("gtm interceptor: error duplicating POST fetch:", error);
                  }
                );
              });
            }
            return originalPromise;
          }
          return originalFetch.apply(this, arguments);
        };
      }
    }
    class XhrInterceptor {
      install(ctx) {
        const originalXHROpen = XMLHttpRequest.prototype.open;
        const originalXHRSend = XMLHttpRequest.prototype.send;
        XMLHttpRequest.prototype.open = function(method, url) {
          this._requestMethod = method;
          this._requestUrl = url;
          return originalXHROpen.apply(this, arguments);
        };
        XMLHttpRequest.prototype.send = function(body) {
          if (this._requestUrl && ctx.isTargetUrl(this._requestUrl)) {
            const originalResult = originalXHRSend.apply(this, arguments);
            try {
              const method = (this._requestMethod || "GET").toUpperCase();
              const duplicateUrl = ctx.buildDuplicateUrl(this._requestUrl);
              if (method === "GET") {
                fetch(duplicateUrl, { method: "GET", keepalive: true }).catch((error) => {
                  if (ctx.debug) console.error("gtm interceptor: error duplicating GET xhr:", error);
                });
              } else if (method === "POST") {
                fetch(duplicateUrl, { method: "POST", body, keepalive: true }).catch(
                  (error) => {
                    if (ctx.debug)
                      console.error("gtm interceptor: error duplicating POST xhr:", error);
                  }
                );
              }
            } catch (dupErr) {
              if (ctx.debug) console.error("gtm interceptor: xhr duplication failed:", dupErr);
            }
            return originalResult;
          }
          return originalXHRSend.apply(this, arguments);
        };
      }
    }
    class BeaconInterceptor {
      install(ctx) {
        if (!navigator.sendBeacon) return;
        const originalSendBeacon = navigator.sendBeacon;
        navigator.sendBeacon = function(url, data) {
          if (ctx.isTargetUrl(url)) {
            const originalResult = originalSendBeacon.apply(this, arguments);
            try {
              originalSendBeacon.call(navigator, ctx.buildDuplicateUrl(url), data);
            } catch (e) {
              if (ctx.debug) console.error("gtm interceptor: error duplicating sendBeacon:", e);
            }
            return originalResult;
          }
          return originalSendBeacon.apply(this, arguments);
        };
      }
    }
    class ScriptInterceptor {
      install(ctx) {
        try {
          const scriptSrcDescriptor = Object.getOwnPropertyDescriptor(
            HTMLScriptElement.prototype,
            "src"
          );
          const originalScriptSrcSetter = scriptSrcDescriptor && scriptSrcDescriptor.set;
          const originalScriptSrcGetter = scriptSrcDescriptor && scriptSrcDescriptor.get;
          const originalScriptSetAttribute = HTMLScriptElement.prototype.setAttribute;
          const duplicateIfGA4Url = (urlString) => {
            try {
              if (!ctx.isTargetUrl(urlString)) return;
              fetch(ctx.buildDuplicateUrl(urlString), { method: "GET", keepalive: true }).catch(
                (error) => {
                  if (ctx.debug)
                    console.error("gtm interceptor: error duplicating script GET:", error);
                }
              );
            } catch (e) {
            }
          };
          if (originalScriptSrcSetter && originalScriptSrcGetter) {
            const setter = originalScriptSrcSetter;
            const getter = originalScriptSrcGetter;
            Object.defineProperty(HTMLScriptElement.prototype, "src", {
              configurable: true,
              enumerable: true,
              get: function() {
                return getter.call(this);
              },
              set: function(value) {
                try {
                  const last = this.__ga4LastSrcDuplicated;
                  if (value && value !== last) {
                    duplicateIfGA4Url(String(value));
                    this.__ga4LastSrcDuplicated = String(value);
                  }
                  const self = this;
                  const onloadOnce = function() {
                    try {
                      const finalUrl = self.src;
                      if (finalUrl && finalUrl !== self.__ga4LastSrcDuplicated) {
                        duplicateIfGA4Url(finalUrl);
                        self.__ga4LastSrcDuplicated = finalUrl;
                      }
                    } catch (e) {
                    }
                    self.removeEventListener("load", onloadOnce);
                  };
                  this.addEventListener("load", onloadOnce);
                } catch (e) {
                }
                setter.call(this, value);
              }
            });
          }
          HTMLScriptElement.prototype.setAttribute = function(name, value) {
            try {
              if (String(name).toLowerCase() === "src") {
                const v = String(value);
                const last = this.__ga4LastSrcDuplicated;
                if (v && v !== last) {
                  duplicateIfGA4Url(v);
                  this.__ga4LastSrcDuplicated = v;
                }
                const selfAttr = this;
                const onloadOnceAttr = function() {
                  try {
                    const finalUrlAttr = selfAttr.src;
                    if (finalUrlAttr && finalUrlAttr !== selfAttr.__ga4LastSrcDuplicated) {
                      duplicateIfGA4Url(finalUrlAttr);
                      selfAttr.__ga4LastSrcDuplicated = finalUrlAttr;
                    }
                  } catch (e) {
                  }
                  selfAttr.removeEventListener("load", onloadOnceAttr);
                };
                this.addEventListener("load", onloadOnceAttr);
              }
            } catch (e) {
            }
            return originalScriptSetAttribute.apply(this, arguments);
          };
        } catch (e) {
        }
      }
    }
    if (window.__ga4DuplicatorInitialized) {
      if (options.debug) console.warn("GA4 Duplicator: already initialized.");
      return;
    }
    const destinations = [];
    if (options.destinations && Array.isArray(options.destinations)) {
      for (let i = 0; i < options.destinations.length; i++) {
        destinations.push(options.destinations[i]);
      }
    }
    if (options.server_container_url) {
      destinations.push({
        measurement_id: "*",
        server_container_url: options.server_container_url
      });
    }
    if (destinations.length === 0) {
      console.error("GA4 Duplicator: either server_container_url or destinations array is required");
      return;
    }
    function normalizePath(p) {
      p = String(p || "");
      p = p.replace(/\/+$/, "");
      return p === "" ? "/" : p;
    }
    function matchesId(pattern, id) {
      if (!pattern || pattern === "*") return true;
      try {
        const regexStr = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*");
        return new RegExp("^" + regexStr + "$", "i").test(id);
      } catch (e) {
        return pattern.toLowerCase() === id.toLowerCase();
      }
    }
    function getMeasurementId(url) {
      try {
        const parsed = new URL(url, location.href);
        return parsed.searchParams.get("tid") || parsed.searchParams.get("id") || "";
      } catch (e) {
        const match = url.match(/[?&](?:tid|id)=([^&?#]+)/);
        return match ? decodeURIComponent(match[1]) : "";
      }
    }
    function getDestinationForId(id) {
      for (let i = 0; i < destinations.length; i++) {
        if (matchesId(destinations[i].measurement_id, id)) {
          return destinations[i];
        }
      }
      return null;
    }
    function getDuplicateEndpointUrl(dest) {
      const trackingURL = String(dest.server_container_url || "").trim();
      const u = new URL(trackingURL, location.href);
      u.search = "";
      u.hash = "";
      return u;
    }
    function isTargetUrl(url) {
      if (!url || typeof url !== "string") return false;
      try {
        const parsed = new URL(url, location.href);
        for (let i = 0; i < destinations.length; i++) {
          const duplicateTarget = getDuplicateEndpointUrl(destinations[i]);
          if (parsed.origin === duplicateTarget.origin && normalizePath(parsed.pathname) === normalizePath(duplicateTarget.pathname)) {
            return false;
          }
        }
        const params = parsed.searchParams;
        const hasGtm = params.has("gtm");
        const hasTagExp = params.has("tag_exp");
        const measurementId = params.get("tid") || params.get("id") || "";
        const isMeasurementIdGA4 = /^G-[A-Z0-9]+$/i.test(measurementId);
        return hasGtm && hasTagExp && isMeasurementIdGA4;
      } catch (e) {
        if (typeof url === "string") {
          for (let j = 0; j < destinations.length; j++) {
            try {
              const target = getDuplicateEndpointUrl(destinations[j]);
              const targetNoQuery = target.origin + target.pathname;
              if (url.indexOf(targetNoQuery) !== -1) return false;
            } catch (e2) {
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
    function buildDuplicateUrl(originalUrl) {
      const id = getMeasurementId(originalUrl);
      const dest = getDestinationForId(id);
      if (!dest) return "";
      const dst = getDuplicateEndpointUrl(dest);
      try {
        const src = new URL(originalUrl, location.href);
        dst.search = src.search;
      } catch (e) {
      }
      return dst.toString();
    }
    const context = {
      debug: !!options.debug,
      isTargetUrl,
      buildDuplicateUrl
    };
    const interceptors = [
      new FetchInterceptor(),
      new XhrInterceptor(),
      new BeaconInterceptor(),
      new ScriptInterceptor()
    ];
    for (let i = 0; i < interceptors.length; i++) {
      try {
        interceptors[i].install(context);
      } catch (e) {
        if (options.debug) console.error("GA4 Duplicator: failed to install interceptor", e);
      }
    }
    window.__ga4DuplicatorInitialized = true;
  };
})();
//# sourceMappingURL=gd.js.map
