package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.SimpleFetcher;
import com.github.takayoshi24.magicblackspider.handler.HTMLKafkaServer;
import com.github.takayoshi24.magicblackspider.handler.KafkaPageHandler;
import com.github.takayoshi24.magicblackspider.handler.KafkaProducerWorker;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        // Parametry startowe
        String seed = args.length > 0 ? args[0] : null;
        int maxPages = args.length > 1 ? Integer.parseInt(args[1]) : 1200;
        String kafkaServers = args.length > 2 ? args[2] : "localhost:9092";
        String kafkaTopic = args.length > 3 ? args[3] : "pages";
        int htmlPort = args.length > 4 ? Integer.parseInt(args[4]) : 4567;
        int maxDepth = args.length > 5 ? Integer.parseInt(args[5]) : Integer.MAX_VALUE;

        // Scheduler
        Scheduler scheduler = new Scheduler();

        // Fetcher z timeoutem 7s
        SimpleFetcher fetcher = new SimpleFetcher(7000);

        // Queue from crawler to Kafka producer
        BlockingQueue<String> producerQueue = new LinkedBlockingQueue<>();

        // Queue from Kafka consumer to HTML dashboard — bounded by maxPages so all crawled pages are shown
        BlockingQueue<String> displayQueue = new LinkedBlockingQueue<>(maxPages);

        // PageHandler writes crawled URLs into the producer queue
        PageHandler handler = new KafkaPageHandler(producerQueue, maxDepth);

        // Producer worker reads from producerQueue and publishes to Kafka
        KafkaProducerWorker producerWorker = new KafkaProducerWorker(kafkaServers, kafkaTopic, producerQueue);
        producerWorker.start();

        // HTML server reads from Kafka consumer into displayQueue
        HTMLKafkaServer htmlServer = new HTMLKafkaServer(kafkaServers, kafkaTopic, displayQueue, scheduler);
        htmlServer.startServer(htmlPort);

        // Crawler
        MagicBlackSpider spider = new MagicBlackSpider(scheduler, fetcher, handler, 4, 500);

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

            System.out.println("=== KONIEC CRAWL’A ===");
            System.out.println("Przetworzone: " + spider.getProcessedCount());
            System.out.println("Odwiedzone: " + scheduler.visitedSize());
            System.out.println("Nieprzetworzone URL: " + scheduler.queueSize());
            System.out.println("Błędne / odrzucone: " + scheduler.getRejectedCount());
            System.out.println("Oczekiwanie na kolejne zlecenie crawl’a przez UI...");

            scheduler.reset();
            currentSeed = null; // subsequent crawls start from a URL submitted via the web UI
        }
    }
}
