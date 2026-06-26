package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.SimpleFetcher;
import com.github.takayoshi24.magicblackspider.handler.HTMLKafkaServer;
import com.github.takayoshi24.magicblackspider.handler.KafkaPageHandler;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        // Parametry startowe
        String seed = args.length > 0 ? args[0] : "http://books.toscrape.com";
        int maxPages = args.length > 1 ? Integer.parseInt(args[1]) : 1200;
        String kafkaServers = args.length > 2 ? args[2] : "localhost:9092";
        String kafkaTopic = args.length > 3 ? args[3] : "pages";
        int htmlPort = args.length > 4 ? Integer.parseInt(args[4]) : 4567;

        // Scheduler
        Scheduler scheduler = new Scheduler();

        // Fetcher z timeoutem 7s
        SimpleFetcher fetcher = new SimpleFetcher(7000);

        // Kolejka dla Kafka + HTML servera
        BlockingQueue<String> kafkaQueue = new LinkedBlockingQueue<>();

        // PageHandler
        PageHandler handler = new KafkaPageHandler(kafkaQueue);

        // HTML server uruchomiony równolegle
        HTMLKafkaServer htmlServer = new HTMLKafkaServer(kafkaServers, kafkaTopic, kafkaQueue);
        htmlServer.startServer(htmlPort);

        // Crawler
        MagicBlackSpider spider = new MagicBlackSpider(scheduler, fetcher, handler, 4, 500);

        // Start crawl
        spider.start(seed, maxPages);

        // Po zakończeniu crawl
        System.out.println("=== KONIEC CRAWL’A ===");
        System.out.println("Przetworzone: " + spider.getProcessedCount());
        System.out.println("Odwiedzone: " + scheduler.visitedSize());
        System.out.println("Nieprzetworzone URL: " + scheduler.queueSize());
        System.out.println("Błędne / odrzucone: " + scheduler.getRejectedCount());

        htmlServer.stopServer();
    }
}
