[![CI](https://github.com/takayoshi24/MagicBlackSpider/actions/workflows/ci.yml/badge.svg)](https://github.com/takayoshi24/MagicBlackSpider/actions/workflows/ci.yml)

```md
# Java Crawler Framework - szkic

http://books.toscrape.com
Prosty, edukacyjny framework crawlera.

## CLI Usage

```
java -jar magicblackspider.jar [seed] [maxPages] [kafkaServers] [kafkaTopic] [htmlPort] [maxDepth] [threads]
```

| Argument      | Default          | Description                              |
|---------------|------------------|------------------------------------------|
| seed          | _(none)_         | Starting URL (can be submitted via UI)   |
| maxPages      | 1200             | Maximum pages to crawl                   |
| kafkaServers  | localhost:9095   | Kafka bootstrap servers                  |
| kafkaTopic    | pages            | Kafka topic name                         |
| htmlPort      | 4567             | Port for the web dashboard               |
| maxDepth      | _(unlimited)_    | Maximum crawl depth                      |
| threads       | 4                | Crawler thread pool size                 |

Example — 8 threads, depth-limited to 3:
```
java -jar magicblackspider.jar http://books.toscrape.com 500 localhost:9095 pages 4567 3 8
```