function pushPageView() {
  window.dataLayer = window.dataLayer || [];
  window.dataLayer.push({
    event: "page_view",
    page_title: document.title,
    page_location: window.location.href,
    page_referrer: document.referrer,
  });
}

function pushPageViewDelayed() {
  // wait one tick to make sure that document.title is updated
  setTimeout(pushPageView, 0);
}

if (typeof window !== "undefined") {
  if (document.readyState === "complete") {
    pushPageViewDelayed();
  } else {
    window.addEventListener("load", pushPageViewDelayed);
  }
}

export function onRouteDidUpdate({ location, previousLocation }) {
  if (previousLocation) {
    pushPageViewDelayed();
  }
}
