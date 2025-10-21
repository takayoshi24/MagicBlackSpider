package com.github.takayoshi24.magicblackspider;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import com.github.takayoshi24.magicblackspider.handler.HTMLKafkaServer;
import com.github.takayoshi24.magicblackspider.fetcher.SimpleFetcher;
import com.github.takayoshi24.magicblackspider.handler.KafkaPageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    public static void main(String[] args) {
        String seed = args.length > 0 ? args[0] : "http://books.toscrape.com";
        int maxPages = args.length > 1 ? Integer.parseInt(args[1]) : 600;
        String kafkaServers = args.length > 2 ? args[2] : "localhost:9092";
        String kafkaTopic = args.length > 3 ? args[3] : "pages";
        int htmlPort = args.length > 4 ? Integer.parseInt(args[4]) : 4567;

        Scheduler scheduler = new Scheduler();
        SimpleFetcher fetcher = new SimpleFetcher(7000);
        KafkaPageHandler handler = new KafkaPageHandler(kafkaServers, kafkaTopic);

        MagicBlackSpider spider = new MagicBlackSpider(scheduler, fetcher, handler, 4, 500);

        // kolejka dla HTML
        BlockingQueue<String> htmlQueue = new LinkedBlockingQueue<>();
        HTMLKafkaServer htmlServer = new HTMLKafkaServer(kafkaServers, kafkaTopic, htmlQueue);
        htmlServer.startServer(htmlPort);

        spider.start(seed, maxPages);
        handler.close();
        System.out.println("Koniec crawl'a. Przetworzone: " + scheduler.visitedSize());
    }
}