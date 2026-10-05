# rotating-proxy-setups# python rotating proxy: working requests, aiohttp and Scrapy setups for scrapers that keep getting 403s

You can write the rotation logic for `python rotating proxy` in about six lines. The hard part is everything around it: which IPs you're rotating through, when you decide to rotate, and what you do when the response you get back is a block page rather than data.

Most tutorials stop at `itertools.cycle` over a list of proxies in a CSV file. That works fine for a demo and falls apart the first time your free list has nine dead proxies and one honeypot. Below are the working patterns for `requests`, `aiohttp` and Scrapy, plus the parts that actually determine whether your job finishes — retry logic, session stickiness, and where the exit IPs come from.

## Rotation is two different problems wearing one name

Before the code, it's worth separating two things that get bundled together:

**Rotating your own proxy list.** You have a pool, you hand out a different proxy per request or per retry, you evict the dead ones. All the logic is yours.

**Rotating through a provider endpoint.** You have one host, one port and one set of credentials. The provider's network changes the exit IP on each request. There is no list to maintain, and no eviction code, because you never see the individual IPs.

Both show up in Python code very differently, and mixing them up is why people end up with a 200-line "proxy manager" class that still gets rate-limited.

## Option A: keeping your own pool in Python

This is the honest version of the tutorial that shows up everywhere, with the failure handling that usually gets left out.

python
import requests
from itertools import cycle

PROXIES = [
    "http://user:pass@proxy1.example.com:8080",
    "http://user:pass@proxy2.example.com:8080",
    "http://user:pass@proxy3.example.com:8080",
]

pool = cycle(PROXIES)
BAD_STATUS = {403, 429, 503}

def fetch(url, attempts=6, timeout=20):
    last_error = None
    for _ in range(min(attempts, len(PROXIES) * 2)):
        proxy = next(pool)
        proxies = {"http": proxy, "https": proxy}
        try:
            resp = requests.get(url, proxies=proxies, timeout=timeout)
            if resp.status_code in BAD_STATUS:
                last_error = f"HTTP {resp.status_code} via {proxy}"
                continue          # rotate away from this IP
            return resp
        except requests.exceptions.RequestException as exc:
            last_error = f"{type(exc).__name__} via {proxy}"
    raise RuntimeError(f"every attempt failed: {last_error}")


Two things matter here more than the rotation itself.

First, `requests` doesn't retry anything by default. You can bolt on `urllib3`'s `Retry` through an `HTTPAdapter`, but the adapter retries on the *same* proxy — which is exactly wrong when the reason you got a 403 is the IP. Rotate first, retry second.

Second, treat 403 and 429 as rotation triggers, not as exceptions. A 403 from an anti-bot layer is that IP being burned. Sending four more requests through it doesn't help; it just makes the next block more certain.

The obvious weakness of Option A is where the list comes from. Free proxy lists are the worst option you can pick: they're slow, they're shared with everyone else scraping the same targets, and a meaningful share of them will happily inspect your HTTPS traffic. If you're maintaining your own list, buy the IPs.

## Option B: one endpoint, provider-side rotation

With a rotating endpoint you skip the pool entirely. Every request to the same host and port exits through a different IP.

python
import os
import requests

USER = os.environ["PROXY_USER"]
PASS = os.environ["PROXY_PASS"]
GATEWAY = "gw.dataimpulse.com:823"   # HTTP/HTTPS rotating port

proxy_url = f"http://{USER}:{PASS}@{GATEWAY}"
proxies = {"http": proxy_url, "https": proxy_url}

for i in range(5):
    r = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=15)
    print(i, r.json()["ip"])


On DataImpulse, port **823** is the HTTP/HTTPS rotating port and **824** is the SOCKS5 rotating port, so the only thing your code needs is the right port number. Run the loop above and you'll see a different IP each time without a single line of rotation logic in your own script.

Country targeting is done inside the username rather than a separate dashboard setting:

python
# exit IPs from Germany
proxy_url = f"http://{USER}__cr.de:{PASS}@gw.dataimpulse.com:823"


Country-level targeting is included in the per-GB rate. City, state, ZIP and ASN targeting on standard residential plans are billed at **double the standard per-GB rate**, which is worth knowing before you build a pipeline that filters by ZIP code on every request.

👉 [Try DataImpulse residential proxies from $1/GB](https://bit.ly/dataimPulse) — pay-as-you-go, and the GB you buy don't expire.

## Sticky sessions: the same hotel room, for a while

Pure per-request rotation breaks anything stateful. Logins, carts, multi-step forms and paginated flows that check your session all assume the same client is coming back. Rotating the IP mid-flow is a fast way to get logged out.

That's what a sticky session is for. You keep one IP for a defined window by adding a session tag to the username:

python
# same exit IP for the life of this session tag
proxy_url = f"http://{USER}__cr.us__sid.order123:{PASS}@gw.dataimpulse.com:10000"

session = requests.Session()
for page in range(1, 6):
    r = session.get(f"https://example.com/orders?page={page}",
                    proxies={"https": proxy_url}, timeout=20)


The exact string is generated in your dashboard, so copy it from there instead of assembling it by hand. On DataImpulse, sticky connections live in the **10000–20000** port range, hold an IP for **1 to 120 minutes**, and default to **30 minutes** if you don't specify an interval.

The practical rule: rotate per request when each request stands alone (product pages, SERPs, price checks), and hold a session when the target ties requests together. Getting this backwards is the single most common cause of "my scraper works locally but logs me out in production."

## Doing it concurrently with aiohttp

Per-request rotation and async I/O go together well, because each coroutine can carry its own proxy URL:

python
import asyncio
import aiohttp

PROXY = f"http://{USER}:{PASS}@gw.dataimpulse.com:823"
TIMEOUT = aiohttp.ClientTimeout(total=25)

async def fetch(session, url):
    for attempt in range(3):
        try:
            async with session.get(url, proxy=PROXY, timeout=TIMEOUT) as r:
                if r.status in (403, 429):
                    await asyncio.sleep(1 + attempt)
                    continue          # next attempt gets a fresh exit IP
                return await r.text()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(1 + attempt)
    return None

async def main(urls):
    sem = asyncio.Semaphore(20)          # cap concurrency
    async with aiohttp.ClientSession() as session:
        async def bounded(u):
            async with sem:
                return await fetch(session, u)
        return await asyncio.gather(*(bounded(u) for u in urls), return_exceptions=True)

asyncio.run(main([f"https://example.com/item/{i}" for i in range(200)]))


The semaphore is not decoration. With a rotating endpoint there's no proxy list to exhaust, so nothing stops a stray `gather` over 50,000 URLs from opening 50,000 connections and getting your account throttled — by your target *and* by your proxy provider. Cap it, and give each attempt a small backoff rather than hammering immediately.

## Scrapy: middleware or a single rotating endpoint

If you're running your own pool inside Scrapy, rotation belongs in downloader middleware, not in your spiders. That's what `scrapy-rotating-proxies` does: one middleware assigns a proxy per request and a second one decides whether a response looks like a ban, then rotates away from the failing IP.

python
# settings.py
ROTATING_PROXY_LIST = [
    "http://user:pass@proxy1.example.com:8000",
    "http://user:pass@proxy2.example.com:8000",
]
DOWNLOADER_MIDDLEWARES = {
    "rotating_proxies.middlewares.RotatingProxyMiddleware": 610,
    "rotating_proxies.middlewares.BanDetectionMiddleware": 620,
}
ROTATING_PROXY_PAGE_RETRY_TIMES = 5


With a provider endpoint you don't need any of that — Scrapy's own `HttpProxyMiddleware` reads the `http_proxy` and `https_proxy` settings, so a single endpoint covers every request:

python
# settings.py
HTTP_PROXY = f"http://{USER}:{PASS}@gw.dataimpulse.com:823"
DOWNLOAD_TIMEOUT = 25
RETRY_TIMES = 3
RETRY_HTTP_CODES = [429, 500, 502, 503, 504, 522, 524]


Keep `ROTATING_PROXY_PAGE_RETRY_TIMES` (how many *different* proxies a page gets) and `RETRY_TIMES` (how many times Scrapy retries a failed request) mentally separate. They're different knobs in the same pipeline, and setting one while ignoring the other is how you end up with a job that retries five times against the same dead proxy.

## Rotate when the site pushes back, not on a timer

A fixed rotation interval feels tidy and usually wastes money. Every rotation has a cost: the new IP is cold, and if your target is fingerprinting more than IP address, a fresh address from an unknown client isn't automatically more trustworthy.

The better trigger is the response. Rotate on a 403, a 429, or a CAPTCHA page. If you're pulling a page every two seconds and getting clean 200s, there's no reason to change IPs at all — you're just spending bandwidth to look more suspicious.

That also decides a lot of the cost math. A residential IP that returns the data you need at $1/GB beats a cheaper IP that returns block pages, because the only number that matters is cost per *successful* request. Which is the argument for starting small: buy the minimum, point it at your actual target, and measure. Don't model it from a benchmark table.

## What the exit IPs cost

DataImpulse runs a pay-as-you-go model — no subscription, and unused traffic doesn't expire, so a scraper that runs heavy one week and idle the next isn't paying for GB it never used. Here's the current lineup:

| Plan | What you get | Price | Billing | Buy |
| --- | --- | --- | --- | --- |
| Residential | 90M+ ethically sourced IPs across 195 countries; rotating (port 823 HTTP/HTTPS, 824 SOCKS5) and sticky sessions (ports 10000–20000, 1–120 min); HTTP, HTTPS, SOCKS5; country targeting included | $1/GB; entry plan **$5 for 5 GB**; **$0.80/GB** at 1 TB | Pay-as-you-go, no subscription, traffic never expires | [Buy residential traffic](https://bit.ly/dataimPulse) |
| Mobile | Mobile IPs for targets that reject residential ranges; HTTP, HTTPS, SOCKS5 | From $2/GB; **$1.60/GB** at 1 TB | Pay-as-you-go, traffic never expires | [Buy mobile traffic](https://bit.ly/dataimPulse) |
| Datacenter | The cheapest and fastest tier; HTTP, HTTPS, SOCKS5 | From $0.50/GB; **$0.45/GB** at 1 TB | Pay-as-you-go, traffic never expires | [Buy datacenter traffic](https://bit.ly/dataimPulse) |
| Premium Residential | High-speed, high-trust residential pool, dedicated account manager, all targeting options included | From $5/GB; **$5 for 1 GB**, **$50 for 10 GB**; custom pricing from $20,000 for 5 TB+ | Pay-as-you-go, no subscription | [Buy premium residential](https://dataimpulse.com/premium-residential-proxies/?aff=86938) |

A few notes that don't fit in a table cell:

- **Advanced targeting costs extra on standard residential.** City, state, ZIP and ASN filters are billed at 2× the per-GB rate. Country targeting is free, so a pipeline that only needs a country code pays nothing above $1/GB.
- **New users get a 7-day refund window** on the entry plan, excluding crypto payments.
- **Datacenter is worth testing first.** If your target doesn't aggressively fingerprint IP type, $0.50/GB gets the same data for half the price. Most anti-bot stacks *do* flag datacenter ranges, which is why residential exists — but "most" isn't "yours."
- **Authentication works two ways:** username/password in the URL, or an IP whitelist if you'd rather not embed credentials in your config.
- The vendor publishes a 99.51% success rate, a 90M+ IP pool and a 4.8/5 rating on G2. Those are DataImpulse's own numbers, not an independent audit — useful as a baseline, not as proof.

👉 [Grab 5 GB of residential traffic for $5](https://bit.ly/dataimPulse) and run it against your own hardest target before you commit to a volume tier. At $1/GB, 5 GB is roughly 25,000–50,000 typical HTML page fetches, which is enough to find out whether your cost per successful request works.

## Matching plans to Python workloads

**A scraping tutorial or a one-off job under 50 GB/month.** Residential, $1/GB, $5 starter. No subscription means you don't pay for March in April, and non-expiring traffic means a failed experiment isn't money burned.

**SERP collection, price monitoring across many regions.** Residential with country targeting. Rotate per request; you don't need a session tag for independent queries.

**Anything with a login.** Sticky sessions on residential, with a session tag that survives the whole flow. Rotating mid-login is the fastest way to get an account flagged.

**Targets that reject residential too.** Mobile at $2/GB. Mobile IPs sit behind carrier NAT alongside real phone users, which makes them slow to block and also means they're the most expensive per gigabyte. Use them when nothing else gets through, not as a default.

**High-volume, low-friction targets.** Datacenter at $0.50/GB. Concurrency is cheap here; measure the block rate before you assume you need residential.

**Client work with SLA pressure.** Premium Residential, mostly for the dedicated account manager and the included targeting rather than the IPs themselves.

## Small setup mistakes that cost real money

**SOCKS5 needs an extra dependency.** `pip install "requests[socks]"` pulls in PySocks. Without it, `socks5://` URLs fail with an unhelpful error.

**Check your exit IP before you blame the code.** Hit an IP echo service through the proxy and confirm it changed. Half of "rotation doesn't work" reports are a typo'd port or a username missing the `__cr.` separator.

**Don't share a rotator object across threads without a lock.** The cyclic index isn't atomic; you'll get duplicate proxies and skipped ones. If you're using a provider endpoint, this problem disappears — the endpoint *is* the rotator.

**Watch what a session tag does to your data.** If `sid` is held for 30 minutes, requests sent 31 minutes apart land on different IPs and your "session" quietly becomes a rotating one.

**Set timeouts on everything.** A request with no timeout can hang until your process dies. `timeout=20` in `requests`, `ClientTimeout(total=25)` in `aiohttp`, `DOWNLOAD_TIMEOUT` in Scrapy.

## Questions that come up constantly

**Do I need a proxy library at all?** No. A dict with `http` and `https` keys is the whole API in `requests`. Libraries like `rotating-mitmproxy` and `proxywhirl` earn their place when you're juggling your own large pool with health scoring — not when you have one endpoint.

**How many proxies do I need?** With provider-side rotation, none that you can name. That's the point. You need a concurrency cap, a retry policy, and enough GB to finish the job.

**Can I rotate user agents too?** You can, and it's a separate mechanism — headers don't change the IP. Rotating both together is common, but a mismatched User-Agent-to-IP pair across requests is its own detection signal, so consistency matters more than variety for stateful flows.

**Is rotating on every request always right?** No. It's right when requests are independent. For anything that looks like a session to the target, hold the IP and stay consistent.

## Where to start

Write the retry loop first, with real handling for 403 and 429. Then pick the rotation model: your own pool if you want to control which IPs you use, or a single rotating endpoint if you'd rather not maintain a list at all. Add sticky sessions only where a flow actually needs continuity.

Then run it against the target that's been blocking you. The code is the easy half; the IPs are the half that decides whether your Python scraper finishes the job or spends the afternoon collecting block pages.

👉 [Start with 5 GB of DataImpulse residential traffic for $5](https://bit.ly/dataimPulse) — pay-as-you-go, country targeting included, traffic that doesn't expire while you tune the script.
