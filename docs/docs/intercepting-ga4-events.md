# Guide: Intercepting GA4 Events

This guide covers methods for intercepting Google Analytics 4 (GA4) events. Choose the method that best fits your needs.

---

## Method 1: Duplicate GA4 Requests (Recommended)

This method keeps your existing GA4 setup intact while duplicating events to a custom endpoint. Your data will continue flowing to Google Analytics as normal.

### Steps:

1. **Open Google Tag Manager**
   - Navigate to your GTM container
   - Go to the **Tags** section

2. **Create a Custom HTML Tag**
   - Click **New** to create a new tag
   - Choose **Custom HTML** as the tag type
   - Name it: `GA4 Request Duplicator`

3. **Add Your Interception Code**
   - Paste your custom code into the HTML field (Note: remember to edit the line containing endpoint URL!):
   
```html
<script>
(function () {

  var hitbox_endpoint = "https://example.org/g/collect"; // Replace it with your endpoint URL

  function isGA4CollectUrl(url) {
    if (!url || typeof url !== "string") return false;
    try {
      var parsed = new URL(url, location.href);
      // Recursion guard: do not intercept our own duplicate endpoint
      var duplicateTarget = new URL(hitbox_endpoint, location.href);
      if (
        parsed.origin === duplicateTarget.origin &&
        parsed.pathname === duplicateTarget.pathname
      ) {
        return false;
      }

      var params = parsed.searchParams;
      var hasGtm = params.has("gtm");
      var hasTagExp = params.has("tag_exp");
      var measurementId = params.get("tid") || params.get("id") || "";
      var isMeasurementIdGA4 = /^G-[A-Z0-9]+$/i.test(measurementId);

      return hasGtm && hasTagExp && isMeasurementIdGA4;
    } catch (e) {
      // Fallback for non-standard/relative URLs
      if (typeof url === "string") {
        if (url.indexOf(hitbox_endpoint) !== -1) return false;
        var hasGtmFallback = url.indexOf("gtm=") !== -1;
        var hasTagExpFallback = url.indexOf("tag_exp=") !== -1;
        var idMatch = url.match(/[?&](?:tid|id)=G-[A-Za-z0-9]+/);
        return !!(hasGtmFallback && hasTagExpFallback && idMatch);
      }
      return false;
    }
  }

  // --- monkey-patch for XMLHttpRequest --
  var originalXHROpen = XMLHttpRequest.prototype.open;
  var originalXHRSend = XMLHttpRequest.prototype.send;

  XMLHttpRequest.prototype.open = function (method, url) {
    this._requestMethod = method;
    this._requestUrl = url;
    return originalXHROpen.apply(this, arguments);
  };

  XMLHttpRequest.prototype.send = function (body) {
    if (this._requestUrl && isGA4CollectUrl(this._requestUrl)) {
      // First send original request to Google Analytics
      var originalResult = originalXHRSend.apply(this, arguments);
      // Then send duplicate to our endpoint mimicking method and payload
      try {
        var method = (this._requestMethod || "GET").toUpperCase();
        if (method === "GET") {
          var parsedUrl = new URL(this._requestUrl, location.href);
          var dupUrl = new URL(hitbox_endpoint, location.href);
          dupUrl.search = parsedUrl.search; // copy exact query string
          fetch(dupUrl.toString(), {
            method: "GET",
            keepalive: true,
          }).catch(function (error) {
            console.error(
              "gtm /g/collect interceptor: error duplicating GET xhr:",
              error
            );
          });
        } else if (method === "POST") {
          // Reuse the original body as-is and preserve original query string
          var parsedPostUrl = new URL(this._requestUrl, location.href);
          var dupPostUrl = new URL(hitbox_endpoint, location.href);
          dupPostUrl.search = parsedPostUrl.search;
          fetch(dupPostUrl.toString(), {
            method: "POST",
            body: body,
            keepalive: true,
          }).catch(function (error) {
            console.error(
              "gtm /g/collect interceptor: error duplicating POST xhr:",
              error
            );
          });
        }
      } catch (dupErr) {
        console.error(
          "gtm /g/collect interceptor: xhr duplication failed:",
          dupErr
        );
      }
      return originalResult;
    }
    return originalXHRSend.apply(this, arguments);
  };

  // --- monkey-patch for fetch ---
  var originalFetch = window.fetch;
  window.fetch = function (resource, config) {
    var requestUrl =
      typeof resource === "string" ? resource : resource && resource.url;
    var method =
      (config && config.method) || (resource && resource.method) || "GET";

    if (isGA4CollectUrl(requestUrl)) {
      // Prepare duplicate details
      var upperMethod = (method || "GET").toUpperCase();

      // If we need the body from a Request, clone BEFORE calling the original fetch
      var prepareBodyPromise = Promise.resolve(undefined);
      if (upperMethod === "POST") {
        if (config && Object.prototype.hasOwnProperty.call(config, "body")) {
          prepareBodyPromise = Promise.resolve(config.body);
        } else if (
          typeof Request !== "undefined" &&
          resource instanceof Request
        ) {
          try {
            var clonedReq = resource.clone();
            prepareBodyPromise = clonedReq.blob().catch(function () {
              return undefined;
            });
          } catch (e) {
            prepareBodyPromise = Promise.resolve(undefined);
          }
        }
      }

      // First send original request to Google Analytics
      var originalPromise = originalFetch.apply(this, arguments);

      // Compute duplicate URL (copy exact query string)
      var duplicateUrl;
      try {
        var parsed = new URL(requestUrl, location.href);
        var dup = new URL(hitbox_endpoint, location.href);
        dup.search = parsed.search;
        duplicateUrl = dup.toString();
      } catch (e) {
        duplicateUrl = hitbox_endpoint;
      }

      // Then send duplicate to our endpoint mimicking method and payload
      if (upperMethod === "GET") {
        fetch(duplicateUrl, { method: "GET", keepalive: true }).catch(function (
          error
        ) {
          console.error(
            "gtm /g/collect interceptor: error duplicating GET fetch:",
            error
          );
        });
      } else if (upperMethod === "POST") {
        prepareBodyPromise.then(function (dupBody) {
          // If we cloned a Request and got a Blob, use it; otherwise use provided body
          fetch(duplicateUrl, {
            method: "POST",
            body: dupBody,
            keepalive: true,
          }).catch(function (error) {
            console.error(
              "gtm /g/collect interceptor: error duplicating POST fetch:",
              error
            );
          });
        });
      }
      return originalPromise;
    }
    return originalFetch.apply(this, arguments);
  };

  // --- monkey-patch for navigator.sendBeacon ---
  if (navigator.sendBeacon) {
    var originalSendBeacon = navigator.sendBeacon;
    navigator.sendBeacon = function (url, data) {
      if (isGA4CollectUrl(url)) {
        // First send original request to Google Analytics
        var originalResult = originalSendBeacon.apply(this, arguments);
        // Then send duplicate to our endpoint with the same body and query string
        try {
          var parsedBeaconUrl = new URL(url, location.href);
          var dupBeaconUrl = new URL(hitbox_endpoint, location.href);
          dupBeaconUrl.search = parsedBeaconUrl.search;
          originalSendBeacon.call(navigator, dupBeaconUrl.toString(), data);
        } catch (e) {
          console.error(
            "gtm /g/collect interceptor: error duplicating sendBeacon:",
            e
          );
        }
        return originalResult;
      }
      return originalSendBeacon.apply(this, arguments);
    };
  }

  // --- monkey-patch for <script src=...> loads ---
  try {
    var scriptSrcDescriptor = Object.getOwnPropertyDescriptor(
      HTMLScriptElement.prototype,
      "src"
    );
    var originalScriptSrcSetter =
      scriptSrcDescriptor && scriptSrcDescriptor.set;
    var originalScriptSrcGetter =
      scriptSrcDescriptor && scriptSrcDescriptor.get;
    var originalScriptSetAttribute = HTMLScriptElement.prototype.setAttribute;

    var duplicateIfGA4Url = function (urlString) {
      try {
        if (!isGA4CollectUrl(urlString)) return;
        var parsed = new URL(urlString, location.href);
        var dup = new URL(hitbox_endpoint, location.href);
        dup.search = parsed.search;
        fetch(dup.toString(), { method: "GET", keepalive: true }).catch(
          function (error) {
            console.error(
              "gtm /g/collect interceptor: error duplicating script GET:",
              error
            );
          }
        );
      } catch (e) {
        // swallow
      }
    };

    if (originalScriptSrcSetter && originalScriptSrcGetter) {
      Object.defineProperty(HTMLScriptElement.prototype, "src", {
        configurable: true,
        enumerable: true,
        get: function () {
          return originalScriptSrcGetter.call(this);
        },
        set: function (value) {
          try {
            var last = this.__ga4LastSrcDuplicated;
            if (value && value !== last) {
              duplicateIfGA4Url(String(value));
              this.__ga4LastSrcDuplicated = String(value);
            }
            // Also duplicate after load to capture redirected/final URL with query params
            var self = this;
            var onloadOnce = function () {
              try {
                var finalUrl = self.src;
                if (finalUrl && finalUrl !== self.__ga4LastSrcDuplicated) {
                  duplicateIfGA4Url(finalUrl);
                  self.__ga4LastSrcDuplicated = finalUrl;
                }
              } catch (_) {}
              self.removeEventListener("load", onloadOnce);
            };
            this.addEventListener("load", onloadOnce);
          } catch (_) {}
          return originalScriptSrcSetter.call(this, value);
        },
      });
    }

    HTMLScriptElement.prototype.setAttribute = function (name, value) {
      try {
        if (String(name).toLowerCase() === "src") {
          var v = String(value);
          var last = this.__ga4LastSrcDuplicated;
          if (v && v !== last) {
            duplicateIfGA4Url(v);
            this.__ga4LastSrcDuplicated = v;
          }
          // Also attach a load listener to capture final URL
          var selfAttr = this;
          var onloadOnceAttr = function () {
            try {
              var finalUrlAttr = selfAttr.src;
              if (
                finalUrlAttr &&
                finalUrlAttr !== selfAttr.__ga4LastSrcDuplicated
              ) {
                duplicateIfGA4Url(finalUrlAttr);
                selfAttr.__ga4LastSrcDuplicated = finalUrlAttr;
              }
            } catch (_) {}
            selfAttr.removeEventListener("load", onloadOnceAttr);
          };
          this.addEventListener("load", onloadOnceAttr);
        }
      } catch (_) {}
      return originalScriptSetAttribute.apply(this, arguments);
    };
  } catch (e) {
    // ignore environment where HTMLScriptElement may not be available
  }
})();</script> 
```

4. **Configure the Trigger**
   - In the **Triggering** section, click to add a trigger
   - Select **Initialization - All Pages**
   - This ensures the code runs before any GA4 events fire

5. **Set Up Tag Sequencing**
   - In the tag configuration, expand **Advanced Settings**
   - Go to **Tag Sequencing**
   - Under "Setup Tag", select your main **Google Tag** (your GA4 configuration tag)
   - This ensures the GA4 tag loads first, then your duplicator runs

6. **Save and Publish**
   - Save your tag
   - Submit the changes and publish your container
   - Test thoroughly to ensure both GA4 and your custom endpoint receive events

---

## Method 2: Redirect All GA4 Requests

⚠️ **Warning:** This method will stop sending data to your GA4 property. Use only if you want to completely redirect tracking to your own system.

### Steps:

1. **Open Google Tag Manager**
   - Navigate to your GTM container
   - Go to the **Tags** section

2. **Modify Your Google Tag**
   - Find your existing **Google Tag** (GA4 Configuration tag)
   - Edit the tag configuration

3. **Change the Server Container URL**
   - In the tag Configuration settings, add a new parameter called `server_container_url`
   - Set its value to your endpoint URL, e.g.  `https://example.org/g/collect`


---

## Choosing the Right Method

| Factor | Method 1 (Duplicate) | Method 2 (Redirect) |
|--------|---------------------|---------------------|
| **GA4 Data Collection** | ✅ Continues normally | ❌ Stops completely |
| **Use Case** | Testing, data warehousing, dual tracking | Full ownership, GDPR/HIPAA compliance |

---

## Testing Your Setup

After implementing either method:

1. **Open Browser DevTools**
   - Go to Network tab
   - Filter by "collect" or your endpoint domain

2. **Trigger Test Events**
   - Navigate pages, click buttons, submit forms
   - Watch for outgoing requests

3. **Verify Data Flow**
   - Method 1: Check both GA4 and your endpoint receive data
   - Method 2: Check only your endpoint receives data

4. **Use GTM Preview Mode**
   - Enable Preview mode in GTM
   - Verify tags fire in the correct sequence
   - Check for any errors in the console
