import http from 'k6/http';
import { check, sleep, group } from 'k6';



export const options = {
    vus: 5,      // Number of virtual users
    duration: '10s',  // Test duration
    discardResponseBodies: true,
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)'],
};

// Store previous sessionStamps for potential reuse
const previousSessionStamps = [];

// Available event types based on GA4 common events
const eventTypes = [
    'page_view',
    'scroll',
    'user_engagement',
    'click',
    'file_download',
    'video_start',
    'video_progress',
    'video_complete',
    'form_start',
    'form_submit',
    'search',
    'login',
    'sign_up',
    'purchase',
    'add_to_cart',
    'remove_from_cart',
    'view_item',
    'add_to_wishlist'
];

// Function to generate a random client ID
function generateCid() {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < 12; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

// Function to generate a sessionStamp (IP-like format)
function generateSessionStamp() {
    return generateCid();
}

// Function to get sessionStamp with 1% chance of reusing previous one
function getSessionStamp() {
    if (previousSessionStamps.length > 0 && Math.random() < 0.01) {
        return previousSessionStamps[Math.floor(Math.random() * previousSessionStamps.length)];
    }
    const newStamp = generateSessionStamp();
    previousSessionStamps.push(newStamp);
    // Keep only last 100 stamps to avoid memory issues
    if (previousSessionStamps.length > 100) {
        previousSessionStamps.shift();
    }
    return newStamp;
}

// Function to get random event type
function getRandomEventType() {
    return eventTypes[Math.floor(Math.random() * eventTypes.length)];
}

// Function to generate event-specific parameters
function getEventParameters(eventType) {
    const baseParams = '';

    switch (eventType) {
        case 'scroll':
            return `&epn.percent_scrolled=${Math.floor(Math.random() * 100) + 1}`;
        case 'video_progress':
            return `&epn.video_current_time=${Math.floor(Math.random() * 300)}&epn.video_duration=300`;
        case 'purchase':
            return `&epn.value=${(Math.random() * 1000).toFixed(2)}&eps.currency=USD`;
        case 'search':
            return `&eps.search_term=product${Math.floor(Math.random() * 100)}`;
        case 'click':
            return `&eps.link_text=button${Math.floor(Math.random() * 10)}`;
        default:
            return baseParams;
    }
}

export default function () {
    // Generate session parameters
    const cid = generateCid();
    const sessionStamp = getSessionStamp();
    const sessionId = Math.floor(Date.now() / 1000);
    const hitsCount = Math.floor(Math.random() * 18) + 3; // 3-20 hits

    group('session_requests', function () {
        for (let i = 0; i < hitsCount; i++) {
            const eventType = getRandomEventType();
            const eventParams = getEventParameters(eventType);
            const timestamp = Date.now() + (i * 1000); // Space out events by 1 second
            const sequenceNumber = i + 1;

            // Build URL based on testsessions.sh structure
            const url = `http://localhost:8080/g/collect?v=2&tid=G-5T0Z13HKP4&gtm=45je5580h2v9219555710za200&_p=${timestamp}&gcd=13l3l3l2l1l1&npa=1&dma_cps=syphamo&dma=1&_dtn=bench&_dtv=1337&tag_exp=101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116&cid=${cid}&ul=en-us&sr=1745x982&uaa=x86&uab=64&uafvl=Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171&uamb=0&uam=&uap=Linux&uapv=6.14.4&uaw=0&frm=0&pscdl=noapi&_eu=AAAAAAQ&_s=${sequenceNumber}&sid=${sessionId}&sct=1&seg=1&dl=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html&dr=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html&dt=Food%20Shop&en=${eventType}${eventParams}&_ee=1&tfd=${Math.floor(Math.random() * 10000) + 500}&sessionStamp=${sessionStamp}`;

            const params = {
                headers: {
                    'authority': 'region1.google-analytics.com',
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.8',
                    'content-length': '0',
                    'origin': 'https://d8a-tech.github.io',
                    'priority': 'u=1, i',
                    'referer': 'https://d8a-tech.github.io/',
                    'sec-ch-ua': '"Not(A:Brand";v="24", "Chromium";v="122"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Linux"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'no-cors',
                    'sec-fetch-site': 'cross-site',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36'
                },
                tags: {
                    name: 'CollectEndpoint',
                    event_type: eventType,
                    session_id: sessionId.toString()
                }
            };

            const response = http.post(url, null, params);

            check(response, {
                'status is 204': (r) => r.status === 204,
            }, { event_type: eventType });


        }
    });

    // Small sleep after every run
    sleep();
} 
