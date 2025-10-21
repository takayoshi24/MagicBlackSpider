package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.Fetcher;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import com.github.takayoshi24.magicblackspider.Page;
import com.github.takayoshi24.magicblackspider.utils.PolitenessManager;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker.RobotsTxtRules;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Produkcyjny główny crawler MagicBlackSpider
 */
public class MagicBlackSpider {

    private static final Logger logger = LoggerFactory.getLogger(MagicBlackSpider.class);

    private final Scheduler scheduler;
    private final Fetcher fetcher;
    private final PageHandler handler;
    private final ExecutorService executor;
    private final PolitenessManager politenessManager;
    private final RobotsTxtChecker robotsChecker;
    private final int maxPages;

    // dodajemy pole kafkaQueue i producer, które muszą być inicjalizowane w main lub handlerze
    private final BlockingQueue<String> kafkaQueue;
    private final KafkaProducer<String, String> producer;

    public MagicBlackSpider(Scheduler scheduler, Fetcher fetcher, PageHandler handler,
                            int threads, long politenessMillis, int maxPages,
                            BlockingQueue<String> kafkaQueue,
                            KafkaProducer<String, String> producer) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.handler = handler;
        this.executor = Executors.newFixedThreadPool(threads);
        this.politenessManager = new PolitenessManager(politenessMillis);
        this.robotsChecker = new RobotsTxtChecker();
        this.maxPages = maxPages;

        this.kafkaQueue = kafkaQueue;
        this.producer = producer;
    }

    public void start(String seedUrl) {
        logger.info("=== MagicBlackSpider startuje z: {} ===", seedUrl);
        scheduler.add(seedUrl);

        AtomicInteger processed = new AtomicInteger(0);
        AtomicBoolean running = new AtomicBoolean(true);

        while (running.get()) {
            String url = scheduler.next();

            if (url == null) {
                if (processed.get() >= maxPages) break;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                continue;
            }

            executor.submit(() -> {
                try {
                    String baseUrl = new java.net.URL(url).getProtocol() + "://" + new java.net.URL(url).getHost();
                    RobotsTxtRules rules = robotsChecker.fetchRules(baseUrl);
                    if (!robotsChecker.isAllowed(url, rules, baseUrl)) {
                        logger.info("Robots.txt blokuje: {}", url);
                        return;
                    }

                    politenessManager.ensurePolite(url, rules.crawlDelayMillis);

                    Document doc = fetcher.fetch(url);
                    if (doc == null) {
                        logger.warn("Nie udało się pobrać: {}", url);
                        return;
                    }

                    handler.handle(new Page(url, doc), scheduler);

                    scheduler.markVisited(url);

                    int count = processed.incrementAndGet();
                    if (count % 10 == 0) {
                        logger.info("Postęp: {} stron przetworzonych | w kolejce: {} | odwiedzone: {}",
                                count, scheduler.totalAdded() - scheduler.visitedSize(), scheduler.visitedSize());
                    }

                    if (count >= maxPages) {
                        running.set(false);
                    }

                } catch (Exception e) {
                    logger.error("Błąd podczas przetwarzania {}: {}", url, e.getMessage());
                }
            });
        }

        // teraz bezpieczne zamknięcie executor i KafkaProducer
        shutdownExecutorAndKafka();
        logger.info("=== Crawl zakończony. Przetworzono: {} stron ===", processed.get());
    }

    private void shutdownExecutorAndKafka() {
        // nie przyjmujemy nowych zadań
        executor.shutdown();
        try {
            // czekamy aż wszystkie wątki crawl zakończą pracę
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // oznaczamy, że crawl zakończony - wątki wysyłające do Kafki mogą zakończyć
        // dopiero teraz opróżniamy kolejkę i zamykamy producenta
        while (!kafkaQueue.isEmpty()) {
            String msg = kafkaQueue.poll();
            if (msg != null) {
                try {
                    producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("topic", msg));
                } catch (Exception ex) {
                    logger.error("Błąd wysyłki do Kafka podczas zamykania: {}", ex.getMessage());
                }
            }
        }

        producer.close();
        logger.info("KafkaProducer zamknięty, wszystkie wiadomości wysłane.");
    }
}
