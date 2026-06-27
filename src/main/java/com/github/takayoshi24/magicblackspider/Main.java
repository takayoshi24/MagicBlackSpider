package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.SimpleFetcher;
import com.github.takayoshi24.magicblackspider.handler.HTMLKafkaServer;
import com.github.takayoshi24.magicblackspider.handler.KafkaPageHandler;
import com.github.takayoshi24.magicblackspider.handler.KafkaProducerWorker;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        // Parametry startowe
        String seed = args.length > 0 ? args[0] : null;
        int maxPages = parsePositiveInt(args, 1, 1200, "maxPages");
        String kafkaServers = args.length > 2 ? args[2] : "localhost:9095";
        String kafkaTopic = args.length > 3 ? args[3] : "pages";
        int htmlPort = parsePort(args, 4, 4567, "htmlPort");
        int maxDepth = parsePositiveInt(args, 5, Integer.MAX_VALUE, "maxDepth");

        // Scheduler
        Scheduler scheduler = new Scheduler();

        // Fetcher z timeoutem 7s
        SimpleFetcher fetcher = new SimpleFetcher(7000);

        // Queue from crawler to Kafka producer
        BlockingQueue<String> producerQueue = new LinkedBlockingQueue<>();

        // Queue from Kafka consumer to HTML dashboard — capped independently of maxPages to prevent heap exhaustion
        int displayQueueCapacity = Math.min(maxPages, 10_000);
        BlockingQueue<String> displayQueue = new LinkedBlockingQueue<>(displayQueueCapacity);

        // PageHandler writes crawled URLs into the producer queue
        PageHandler handler = new KafkaPageHandler(producerQueue, maxDepth);

        // Producer worker reads from producerQueue and publishes to Kafka
        KafkaProducerWorker producerWorker = new KafkaProducerWorker(kafkaServers, kafkaTopic, producerQueue);
        producerWorker.start();

        int threadCount = 4;

        // HTML server reads from Kafka consumer into displayQueue
        HTMLKafkaServer htmlServer = new HTMLKafkaServer(kafkaServers, kafkaTopic, displayQueue, scheduler, threadCount);
        htmlServer.startServer(htmlPort);

        // Crawler
        MagicBlackSpider spider = new MagicBlackSpider(scheduler, fetcher, handler, threadCount, 500);
        htmlServer.setSpider(spider);

        // Clean up Kafka and web server on Ctrl+C / SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producerWorker.stop();
            try { producerWorker.awaitStop(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            htmlServer.stopServer();
        }));

        // Run crawl loop — after each crawl finishes, reset and wait for the next seed from the UI
        String currentSeed = seed;
        while (true) {
            spider.start(currentSeed, maxPages);
            htmlServer.signalCrawlFinished();

            logger.info("=== KONIEC CRAWL’A ===");
            logger.info("Przetworzone: {}", spider.getProcessedCount());
            logger.info("Odwiedzone: {}", scheduler.visitedSize());
            logger.info("Nieprzetworzone URL: {}", scheduler.queueSize());
            logger.info("Błędne / odrzucone: {}", scheduler.getRejectedCount());
            logger.info("Oczekiwanie na kolejne zlecenie crawl’a przez UI...");

            scheduler.reset();
            currentSeed = null; // subsequent crawls start from a URL submitted via the web UI
        }
    }

    private static final String USAGE =
        "Usage: <seed> [maxPages] [kafkaServers] [kafkaTopic] [htmlPort] [maxDepth]";

    private static int parsePositiveInt(String[] args, int index, int defaultValue, String name) {
        if (args.length <= index) return defaultValue;
        try {
            int value = Integer.parseInt(args[index]);
            if (value <= 0) {
                logger.error("Error: {} must be > 0 (got {})", name, value);
                logger.error(USAGE);
                System.exit(1);
            }
            return value;
        } catch (NumberFormatException e) {
            logger.error("Error: {} must be an integer (got \"{}\")", name, args[index]);
            logger.error(USAGE);
            System.exit(1);
            return defaultValue; // unreachable, satisfies compiler
        }
    }

    private static int parsePort(String[] args, int index, int defaultValue, String name) {
        if (args.length <= index) return defaultValue;
        try {
            int port = Integer.parseInt(args[index]);
            if (port < 1 || port > 65535) {
                logger.error("Error: {} must be between 1 and 65535 (got {})", name, port);
                logger.error(USAGE);
                System.exit(1);
            }
            return port;
        } catch (NumberFormatException e) {
            logger.error("Error: {} must be an integer (got \"{}\")", name, args[index]);
            logger.error(USAGE);
            System.exit(1);
            return defaultValue; // unreachable, satisfies compiler
        }
    }
}
