package com.github.takayoshi24.magicblackspider;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.github.takayoshi24.magicblackspider.fetcher.SimpleFetcher;
import com.github.takayoshi24.magicblackspider.handler.HTMLKafkaServer;
import com.github.takayoshi24.magicblackspider.handler.KafkaPageHandler;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Główny punkt wejścia MagicBlackSpider — wersja produkcyjna.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        // 🔧 Parametry konfiguracyjne
        String seed = args.length > 0 ? args[0] : "http://books.toscrape.com";
        int maxPages = args.length > 1 ? Integer.parseInt(args[1]) : 600;
        String kafkaServers = args.length > 2 ? args[2] : "localhost:9092";
        String kafkaTopic = args.length > 3 ? args[3] : "magicblackspider-pages";
        int htmlPort = args.length > 4 ? Integer.parseInt(args[4]) : 4567;
        int threads = args.length > 5 ? Integer.parseInt(args[5]) : 4;
        long politenessMillis = args.length > 6 ? Long.parseLong(args[6]) : 500L; // domyślnie 0,5 sekundy

        logger.info("=== Uruchamianie MagicBlackSpider ===");
        logger.info("Seed: {}", seed);
        logger.info("MaxPages: {}", maxPages);
        logger.info("Kafka: {} / Topic: {}", kafkaServers, kafkaTopic);
        logger.info("HTML port: {}", htmlPort);
        logger.info("Wątki: {}, Politeness: {} ms", threads, politenessMillis);

        // ⚙️ Komponenty crawl’a
        Scheduler scheduler = new Scheduler();
        SimpleFetcher fetcher = new SimpleFetcher(7000); // timeout = 7s

        // 🛠 Kolejka i KafkaProducer
        BlockingQueue<String> kafkaQueue = new LinkedBlockingQueue<>();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // handler, który wrzuca dane do kolejki zamiast od razu do Kafki
        PageHandler handler = new KafkaPageHandler(kafkaQueue);

// teraz HTMLKafkaServer używa tej samej kolejki
        HTMLKafkaServer htmlServer = new HTMLKafkaServer(kafkaServers, kafkaTopic, kafkaQueue);
        htmlServer.startServer(htmlPort);

        // 🕷️ Uruchom crawler z nowym konstruktorem, który obsługuje kolejkę i producenta
        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler,
                fetcher,
                handler,
                threads,
                politenessMillis,
                maxPages,
                kafkaQueue,
                producer
        );

        // start crawl’a
        spider.start(seed);

        logger.info("=== Crawl zakończony. Odwiedzone: {} stron ===", scheduler.visitedSize());
        System.out.println("Koniec crawl’a. Przetworzone: " + scheduler.visitedSize());
    }
}
