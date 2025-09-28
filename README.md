# D8A

This is the in-progress tracker for D8a.tech - GA4-compatible analytics platform.

## Setup

run

```bash
go run main.go server
```

make a request and wait a minute for the session to be closed.

```bash
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
```

## Test

```bash
go test ./...
```