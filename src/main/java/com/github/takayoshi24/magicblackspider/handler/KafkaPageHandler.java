package com.github.takayoshi24.magicblackspider.handler;

import com.github.takayoshi24.magicblackspider.Page;
import com.github.takayoshi24.magicblackspider.Scheduler;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.BlockingQueue;

public class KafkaPageHandler implements PageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPageHandler.class);
    private final BlockingQueue<String> kafkaQueue;
    private final int maxDepth;

    public KafkaPageHandler(BlockingQueue<String> kafkaQueue) {
        this(kafkaQueue, Integer.MAX_VALUE);
    }

    public KafkaPageHandler(BlockingQueue<String> kafkaQueue, int maxDepth) {
        this.kafkaQueue = kafkaQueue;
        this.maxDepth = maxDepth;
    }

    public void handle(Page page, Scheduler scheduler, int depth) {
        if (page == null || page.getDocument() == null) return;

        String url = page.getUrl();
        Document doc = page.getDocument();

        // dodaj URL do Kafka w formacie depth|url
        kafkaQueue.offer(depth + "|" + url);

        // pobierz linki
        Elements links = doc.select("a[href]");
        String baseDomain = "";
        try {
            String host = URI.create(url).getHost();
            if (host != null) baseDomain = host;
        } catch (Exception e) {
            logger.warn("Failed to extract domain from URL: {}", url, e);
        }

        int newLinks = 0;
        for (Element link : links) {
            String href = link.absUrl("href");
            if (href.isEmpty() || href.contains("#")) continue;

            try {
                URI newUri = URI.create(href);
                String scheme = newUri.getScheme();
                if (!"http".equals(scheme) && !"https".equals(scheme)) continue;
                String linkHost = newUri.getHost();
                if (linkHost == null || !linkHost.equalsIgnoreCase(baseDomain)) continue;
                if (depth + 1 <= maxDepth && scheduler.add(href, depth + 1)) {
                    newLinks++;
                }
            } catch (Exception e) {
                logger.warn("Failed to parse URL during link extraction: {}", href, e);
            }
        }

        logger.debug("Dodano {} nowych linków z {}", newLinks, url);
    }

    @Override
    public void close() {
        logger.info("KafkaPageHandler zakończył pracę.");
    }
}
