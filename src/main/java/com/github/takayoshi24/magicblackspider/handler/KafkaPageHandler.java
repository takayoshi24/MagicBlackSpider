package com.github.takayoshi24.magicblackspider.handler;

import com.github.takayoshi24.magicblackspider.Scheduler;
import com.github.takayoshi24.magicblackspider.Page;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

/**
 * Produkcyjny KafkaPageHandler przystosowany do pracy z MagicBlackSpider:
 * - wysyła dane strony do kolejki kafkaQueue
 * - analizuje linki i dodaje je do scheduler
 * - ogranicza crawl do jednej domeny
 */
public class KafkaPageHandler implements PageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPageHandler.class);

    private final BlockingQueue<String> kafkaQueue;

    public KafkaPageHandler(BlockingQueue<String> kafkaQueue) {
        this.kafkaQueue = kafkaQueue;
    }

    /**
     * Przetwarza stronę:
     * - dodaje wpisy do kolejki kafkaQueue
     * - dodaje nowe linki do scheduler
     */
    @Override
    public void handle(Page page, Scheduler scheduler) {
        if (page == null || page.getUrl() == null || page.getDocument() == null) return;

        Document doc = page.getDocument();
        String url = page.getUrl();

        // Dodaj dane do kolejki (format: depth|url lub tytuł + treść + url)
        try {
            String title = doc.title();
            String content = doc.text().substring(0, Math.min(doc.text().length(), 500)); // limit treści
            String message = title + " | " + content + " | " + url;
            kafkaQueue.offer(message);
            logger.info("Dodano do kolejki Kafka: {} | {}", title, url);
        } catch (Exception e) {
            logger.error("Błąd przy dodawaniu do kolejki Kafka dla {}: {}", url, e.getMessage());
        }

        // Znajdź i dodaj nowe linki do scheduler
        try {
            URL baseUrl = new URL(url);
            String baseDomain = baseUrl.getHost();

            Elements links = doc.select("a[href]");
            int newLinks = 0;

            for (Element link : links) {
                String href = link.absUrl("href");

                if (href == null || href.isBlank()) continue;
                try {
                    URL newUrl = new URL(href);

                    // Ogranicz do tej samej domeny
                    if (!newUrl.getHost().equalsIgnoreCase(baseDomain)) continue;

                    // Pomiń linki prowadzące do sekcji/anchorów
                    if (href.contains("#")) continue;

                    if (scheduler.add(href)) {
                        newLinks++;
                    }
                } catch (MalformedURLException ignore) {
                }
            }

            logger.debug("Dodano {} nowych linków z {}", newLinks, url);

        } catch (MalformedURLException e) {
            logger.warn("Niepoprawny URL bazowy: {}", url);
        }
    }

    /**
     * Handler nie zamyka producenta — robi to MagicBlackSpider
     */
    public void close() {
        logger.info("KafkaPageHandler: zakończono pracę handlera, producent zamykany przez MagicBlackSpider.");
    }
}
