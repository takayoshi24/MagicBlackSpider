[![CI](https://github.com/takayoshi24/MagicBlackSpider/actions/workflows/ci.yml/badge.svg)](https://github.com/takayoshi24/MagicBlackSpider/actions/workflows/ci.yml)

# MagicBlackSpider

An educational Java web crawler framework. It crawls a target website, publishes discovered URLs to Apache Kafka over TLS, and displays live results in a dark-themed web dashboard. Animated spiders bounce across the screen as pages are found — one per crawl depth, colored by a golden-angle HSL distribution.

---

## Architecture

```
CLI args
   │
   ▼
Main.java
   ├─► Scheduler          — deduplication queue (LRU cap 500k URLs)
   ├─► SimpleFetcher       — HTTP client with retries, redirect following, SSRF guard
   ├─► RobotsTxtChecker    — robots.txt parser & cache (30-min TTL)
   ├─► PolitenessManager   — per-host rate limiter (respects Crawl-Delay)
   ├─► MagicBlackSpider    — multi-threaded crawl loop
   │       └─► KafkaPageHandler   — extracts links + enqueues depth|url to Kafka producer queue
   ├─► KafkaProducerWorker — drains producer queue → Kafka (SSL/TLS)
   └─► HTMLKafkaServer     — Kafka consumer + Javalin web dashboard (SSE live feed)
```

---

## Features

- **Multi-threaded crawling** — configurable thread pool, semaphore-capped at `maxPages`
- **Depth-limited crawl** — stays on the same domain, stops at `maxDepth`
- **Pause / resume** — pause and resume the active crawl from the web UI; timer and crawl rate freeze during pause
- **robots.txt compliance** — fetches and caches rules per host, respects `Disallow`, `Allow`, and `Crawl-Delay`
- **Politeness** — per-host minimum delay (default 500 ms); re-queues URLs whose host isn't ready yet
- **SSRF protection** — blocks crawl/redirect to loopback, link-local, and private (RFC-1918) addresses
- **Kafka integration** — publishes `depth|url` messages over SSL/TLS; consumer feeds the live dashboard
- **Live web dashboard** — real-time table of crawled URLs with depth badges, color-coded by depth
- **Animated spiders** — one spider per depth, hue from golden-angle HSL, size and speed driven by crawl rate
- **Stats panel** — queue size, in-flight count, unique hosts, duplicate link attempts, failed fetches, robots-blocked count, 30-second rolling crawl rate, elapsed timer
- **Depth breakdown panel** — draggable panel showing pages per depth with matching color dots
- **DOCX export** — download a formatted Word report (domain, duration, pages-per-depth table, full URL list)
- **CSV export** — download an RFC 4180 comma-delimited file (`depth,url,host,path`)
- **Kafka error banner** — a non-intrusive banner appears in the UI if the Kafka producer fails to deliver a record
- **Security** — HTTP Basic Auth on the dashboard, CSRF tokens on all POST forms, `Content-Security-Policy` / `X-Frame-Options` / `X-Content-Type-Options` / `Referrer-Policy` headers
- **Graceful shutdown** — `SIGTERM`/`Ctrl+C` flushes and closes Kafka producer/consumer cleanly
- **Multi-crawl loop** — after a crawl finishes the spider resets and waits for the next seed URL from the UI

---

## Requirements

| Requirement | Version |
|-------------|---------|
| Java | 21 |
| Maven | 3.9+ |
| Docker + Compose | for Kafka |

---

## Quick Start

### 1. Generate TLS certificates

The Kafka broker and clients communicate over SSL. A self-signed CA + keystore/truststore is required.

```bash
mkdir certs && cd certs

# Generate CA key and certificate
openssl req -new -x509 -keyout ca.key -out ca.crt -days 365 -nodes \
  -subj "/CN=kafka-ca"

# Generate broker keystore
keytool -genkey -alias kafka -keyalg RSA -keystore kafka.keystore.jks \
  -storepass changeit -keypass changeit -validity 365 \
  -dname "CN=localhost,OU=dev,O=dev,L=dev,ST=dev,C=PL" \
  -ext SAN=DNS:localhost,IP:127.0.0.1

# Sign the broker certificate with the CA
keytool -certreq -alias kafka -keystore kafka.keystore.jks \
  -storepass changeit -file broker.csr

openssl x509 -req -CA ca.crt -CAkey ca.key -in broker.csr \
  -out broker.crt -days 365 -CAcreateserial

keytool -import -alias ca -file ca.crt -keystore kafka.keystore.jks \
  -storepass changeit -noprompt
keytool -import -alias kafka -file broker.crt -keystore kafka.keystore.jks \
  -storepass changeit -noprompt

# Create client truststore with the CA
keytool -import -alias ca -file ca.crt -keystore kafka.truststore.jks \
  -storepass changeit -noprompt

cd ..
```

### 2. Start Kafka

```bash
docker compose up -d
```

Kafka runs on `localhost:9095` (SSL). The `kafka-setup` service creates the `pages` topic automatically.

### 3. Build

```bash
mvn package -DskipTests
```

The fat JAR is produced at `target/MagicBlackSpider-1.0.0.jar`.

### 4. Run

```bash
java -jar target/MagicBlackSpider-1.0.0.jar [seed] [maxPages] [kafkaServers] [kafkaTopic] [htmlPort] [maxDepth] [threads]
```

Open the dashboard at `http://127.0.0.1:4567/` — the credentials (`admin` / `<generated-key>`) are printed to the log at startup.

---

## CLI Arguments

| Position | Argument | Default | Description |
|----------|----------|---------|-------------|
| 0 | `seed` | _(none)_ | Starting URL — can also be submitted via the web UI |
| 1 | `maxPages` | `1200` | Maximum pages to crawl per session |
| 2 | `kafkaServers` | `localhost:9095` | Kafka bootstrap servers |
| 3 | `kafkaTopic` | `pages` | Kafka topic name |
| 4 | `htmlPort` | `4567` | Port for the web dashboard |
| 5 | `maxDepth` | _(unlimited)_ | Maximum crawl depth (links beyond this depth are ignored) |
| 6 | `threads` | `4` | Crawler thread pool size |

All numeric arguments are validated at startup; invalid values print usage and exit.

**Examples:**

```bash
# Crawl with default settings, seed from UI
java -jar target/MagicBlackSpider-1.0.0.jar

# Crawl books.toscrape.com — 500 pages, depth 3, 8 threads
java -jar target/MagicBlackSpider-1.0.0.jar \
  http://books.toscrape.com 500 localhost:9095 pages 4567 3 8
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SSL_TRUSTSTORE_LOCATION` | `certs/kafka.truststore.jks` | Path to the JKS truststore |
| `KAFKA_SSL_STORE_PASSWORD` | `changeit` | Password for keystore and truststore |

---

## Project Structure

```
src/main/java/.../magicblackspider/
├── Main.java                      — entry point, wires all components
├── MagicBlackSpider.java          — crawl loop, thread pool, robots + politeness dispatch
├── Scheduler.java                 — thread-safe URL queue with LRU deduplication (500k cap)
├── Page.java                      — value object (url + parsed Document)
├── fetcher/
│   ├── Fetcher.java               — interface
│   └── SimpleFetcher.java         — Jsoup-based HTTP client (retry, redirect, SSRF guard)
├── handler/
│   ├── PageHandler.java           — interface
│   ├── KafkaPageHandler.java      — extracts same-domain links, publishes depth|url to queue
│   ├── KafkaProducerWorker.java   — background thread draining queue → Kafka producer (SSL)
│   └── HTMLKafkaServer.java       — Javalin web server + Kafka consumer + SSE dashboard
└── utils/
    ├── PolitenessManager.java     — per-host rate limiter (non-blocking tryAcquire)
    └── RobotsTxtChecker.java      — robots.txt fetcher, parser, and cache
```

---

## Web Dashboard

The dashboard is served on `http://127.0.0.1:<htmlPort>/` and requires HTTP Basic Auth.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard HTML |
| `/seed` | POST | Submit a seed URL to start a crawl |
| `/pause` | POST | Pause the active crawl |
| `/resume` | POST | Resume a paused crawl |
| `/clear` | POST | Clear the displayed URL table |
| `/download` | POST | Download DOCX report and clear the table |
| `/download?format=csv` | POST | Download CSV export and clear the table |
| `/events` | GET (SSE) | Server-Sent Events stream (`state`, `url`, `stats`, `clear_table`) |

### Live Stats Panel (right sidebar)

- **Crawl Time** — elapsed timer (pauses when crawl is paused, freezes on finish)
- **Pages crawled** — total URLs received from Kafka
- **Queue** — URLs waiting to be fetched
- **In-flight** — active fetches / thread count
- **Unique hosts** — distinct hostnames seen
- **Dup. link attempts** — total duplicate link attempts discovered across pages
- **Failed fetches** — HTTP errors or timeouts
- **Robots blocked** — URLs denied by robots.txt
- **Crawl rate** — 30-second rolling window (pages/s); shows `0.0/s` while paused

### Depth Panel (draggable)

Shows a colored dot and count for each crawl depth encountered. Positioned automatically below the stats panel; drag to reposition.

### Animated Spiders

While a crawl is running, one spider emoji (`🕷`) bounces per depth. Properties:
- **Color** — golden-angle HSL hue, matching the depth badge color
- **Size** — grows logarithmically with the number of pages at that depth
- **Speed** — proportional to the recent arrival rate for that depth

### Kafka Error Banner

If the Kafka producer fails to deliver a record, a non-intrusive banner appears at the bottom of the dashboard. It does not interrupt the crawl.

---

## Key Design Decisions

**Semaphore-capped crawl loop** — `maxPages` permits are issued up front. Each URL dispatch consumes one permit; none is released on completion. Exceptions in the robots/politeness check release the permit before skipping, so errors never leak slots.

**Dispatch-time politeness** — robots.txt and per-host rate limiting are checked on the main dispatch thread, so worker threads never sleep. A URL that isn't ready is re-queued and the permit is returned.

**Pause / resume** — the UI toggles optimistically (local state flips before the server responds); the server corrects it if the request fails. The timer snapshot (`pausedAtMs`) and accumulator (`totalPausedMs`) exclude all paused time from elapsed calculations and the rolling crawl rate.

**30-second rolling crawl rate** — the client tracks timestamps of incoming `url` SSE events in a sliding window. The rate is `events_in_window / min(30, elapsed_seconds)` and resets to zero when crawl is paused or cleared.

**LRU deduplication** — `Scheduler` uses a `LinkedHashMap` capped at 500k entries. URLs beyond the cap are evicted so memory stays bounded on very large crawls.

**Queue-bounded display buffer** — the in-memory URL list for the dashboard is capped at `min(maxPages, 10000)` entries; oldest entries are dropped if Kafka outpaces consumption.

**5xx vs 4xx handling** — `SimpleFetcher` retries on 5xx responses (transient server errors) but returns `null` immediately on 4xx (permanent client errors). `ignoreHttpErrors(true)` is set on the Jsoup connection so the status code is inspected explicitly rather than relying on exception type.

**CSRF on every mutating endpoint** — a 256-bit random token is embedded as a hidden field on every form. The SSE endpoint uses the same token as a query parameter (browsers cannot send `Authorization` headers on `EventSource`).

---

## Running Tests

```bash
mvn test
```

63 tests covering the crawl loop, scheduler, fetcher retry/SSRF behaviour, robots.txt parsing, politeness dispatch, and dashboard request handling. No running Kafka required — collaborators are stubbed or use the JDK's built-in `HttpServer`. The CI pipeline (GitHub Actions, JDK 21 Temurin) runs tests and packages the JAR on every push and pull request to `main`.

---

## Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| [jsoup](https://jsoup.org/) | 1.18.3 | HTML fetching and link extraction |
| [Apache Kafka Clients](https://kafka.apache.org/) | 4.1.0 | Kafka producer and consumer |
| [Javalin](https://javalin.io/) | 6.3.0 | Embedded HTTP server and SSE |
| [Apache POI](https://poi.apache.org/) | 5.3.0 | DOCX report generation |
| [SLF4J](https://www.slf4j.org/) + [Logback](https://logback.qos.ch/) | 2.0.15 / 1.5.32 | Structured logging |
| [JUnit Jupiter](https://junit.org/junit5/) | 5.10.2 | Unit tests |
