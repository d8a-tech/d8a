#!/bin/bash

BASE_URL=http://localhost:8080

echo "partition 1, request 1"
CID="ag9"
SESSION_STAMP="127.0.0.1"
curl "${BASE_URL}/g/collect?v=2&tid=G-5T0Z13HKP4&gtm=45je5580h2v9219555710za200&_p=1746817938582&gcd=13l3l3l2l1l1&npa=1&dma_cps=syphamo&dma=1&tag_exp=101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116&cid=${CID}&ul=en-us&sr=1745x982&uaa=x86&uab=64&uafvl=Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171&uamb=0&uam=&uap=Linux&uapv=6.14.4&uaw=0&frm=0&pscdl=noapi&_eu=AAAAAAQ&_s=1&sid=1746817858&sct=1&seg=1&dl=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html&dr=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html&dt=Food%20Shop&en=page_view&_ee=1&tfd=565&sessionStamp=${SESSION_STAMP}&ep.content_group=product&ep.content_id=C_1234" \
  -X 'POST' \
  -H 'authority: region1.google-analytics.com' \
  -H 'accept: */*' \
  -H 'accept-language: en-US,en;q=0.8' \
  -H 'content-length: 0' \
  -H 'origin: https://d8a-tech.github.io' \
  -H 'priority: u=1, i' \
  -H 'referer: https://d8a-tech.github.io/' \
  -H 'sec-ch-ua: "Not(A:Brand";v="24", "Chromium";v="122"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Linux"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: no-cors' \
  -H 'sec-fetch-site: cross-site' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36' ;


echo "partition 2, request 1"
CID="ag8"
SESSION_STAMP="127.0.0.2"
curl "${BASE_URL}/g/collect?v=2&tid=G-5T0Z13HKP4&gtm=45je5580h2v9219555710za200&_p=1746817938582&gcd=13l3l3l2l1l1&npa=1&dma_cps=syphamo&dma=1&tag_exp=101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116&cid=${CID}&ul=en-us&sr=1745x982&uaa=x86&uab=64&uafvl=Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171&uamb=0&uam=&uap=Linux&uapv=6.14.4&uaw=0&frm=0&pscdl=noapi&_eu=AEAAAAQ&_s=2&sid=1746817858&sct=1&seg=1&dl=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html&dr=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html&dt=Food%20Shop&en=scroll&epn.percent_scrolled=90&_et=10&tfd=5567&sessionStamp=${SESSION_STAMP}&ep.content_group=product&ep.content_id=C_1234" \
  -X 'POST' \
  -H 'authority: region1.google-analytics.com' \
  -H 'accept: */*' \
  -H 'accept-language: en-US,en;q=0.8' \
  -H 'content-length: 0' \
  -H 'origin: https://d8a-tech.github.io' \
  -H 'priority: u=1, i' \
  -H 'referer: https://d8a-tech.github.io/' \
  -H 'sec-ch-ua: "Not(A:Brand";v="24", "Chromium";v="122"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Linux"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: no-cors' \
  -H 'sec-fetch-site: cross-site' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36' ;


echo "partition 4, request 1 (should be evicted)"
CID="ag7"
SESSION_STAMP="127.0.0.1"
curl "${BASE_URL}/g/collect?v=2&tid=G-5T0Z13HKP4&gtm=45je5580h2v9219555710za200&_p=1746817938582&gcd=13l3l3l2l1l1&npa=1&dma_cps=syphamo&dma=1&tag_exp=101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116&cid=${CID}&ul=en-us&sr=1745x982&uaa=x86&uab=64&uafvl=Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171&uamb=0&uam=&uap=Linux&uapv=6.14.4&uaw=0&frm=0&pscdl=noapi&_eu=AAAAAAQ&_s=1&sid=1746817858&sct=1&seg=1&dl=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html&dr=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html&dt=Food%20Shop&en=page_view&_ee=1&tfd=565&sessionStamp=${SESSION_STAMP}" \
  -X 'POST' \
  -H 'authority: region1.google-analytics.com' \
  -H 'accept: */*' \
  -H 'accept-language: en-US,en;q=0.8' \
  -H 'content-length: 0' \
  -H 'origin: https://d8a-tech.github.io' \
  -H 'priority: u=1, i' \
  -H 'referer: https://d8a-tech.github.io/' \
  -H 'sec-ch-ua: "Not(A:Brand";v="24", "Chromium";v="122"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Linux"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: no-cors' \
  -H 'sec-fetch-site: cross-site' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36' ;


echo "partition 1, (direct) request 2"
CID="ai7"
SESSION_STAMP="127.0.0.3"
curl "${BASE_URL}/g/collect?v=2&tid=G-5T0Z13HKP4&gtm=45je5580h2v9219555710za200&_p=1746817938582&gcd=13l3l3l2l1l1&npa=1&dma_cps=syphamo&dma=1&tag_exp=101509157~103101750~103101752~103116026~103130495~103130497~103200004~103211513~103233427~103251618~103251620~103284320~103284322~103301114~103301116&cid=${CID}&ul=en-us&sr=1745x982&uaa=x86&uab=64&uafvl=Not(A%253ABrand%3B24.0.0.0%7CChromium%3B122.0.6261.171&uamb=0&uam=&uap=Linux&uapv=6.14.4&uaw=0&frm=0&pscdl=noapi&_eu=AAAAAAQ&_s=3&sid=1746817858&sct=1&seg=1&dl=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Findex.html&dr=https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Fcheckout.html&dt=Food%20Shop&en=user_engagement&_et=16002&tfd=16582&sessionStamp=${SESSION_STAMP}" \
  -X 'POST' \
  -H 'authority: region1.google-analytics.com' \
  -H 'accept: */*' \
  -H 'accept-language: en-US,en;q=0.8' \
  -H 'content-length: 0' \
  -H 'origin: https://d8a-tech.github.io' \
  -H 'priority: u=1, i' \
  -H 'referer: https://d8a-tech.github.io/' \
  -H 'sec-ch-ua: "Not(A:Brand";v="24", "Chromium";v="122"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Linux"' \
  -H 'sec-fetch-dest: empty' \
  -H 'X-Forwarded-For: 127.0.0.11' \
  -H 'sec-fetch-mode: no-cors' \
  -H 'sec-fetch-site: cross-site' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/6.8.3 Chrome/122.0.0.0 Safari/537.36' ;
