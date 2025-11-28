function pushPageView() {
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push({
        event: 'virtual_page_view',
        page_title: document.title,
        page_location: window.location.href,
        page_referrer: document.referrer
    });
}

if (typeof window !== 'undefined') {
    if (document.readyState === 'complete') {
        pushPageView();
    } else {
        window.addEventListener('load', pushPageView);
    }
}

export function onRouteDidUpdate({ location, previousLocation }) {
    if (previousLocation) {
        pushPageView();
    }
}