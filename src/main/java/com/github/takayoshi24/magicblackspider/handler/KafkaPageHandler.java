package com.github.takayoshi24.magicblackspider.handler;

import com.github.takayoshi24.magicblackspider.Page;
import com.github.takayoshi24.magicblackspider.Scheduler;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.concurrent.BlockingQueue;

public class KafkaPageHandler implements PageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPageHandler.class);
    private final BlockingQueue<String> kafkaQueue;

    public KafkaPageHandler(BlockingQueue<String> kafkaQueue) {
        this.kafkaQueue = kafkaQueue;
    }

    public void handle(Page page, Scheduler scheduler, int depth) {
        if (page == null || page.getDocument() == null) return;

        String url = page.getUrl();
        Document doc = page.getDocument();

        // dodaj URL do Kafka
        kafkaQueue.offer(url);

        // pobierz linki
        Elements links = doc.select("a[href]");
        String baseDomain = "";
        try {
            baseDomain = new URL(url).getHost();
        } catch (Exception ignored) {}

        int newLinks = 0;
        for (Element link : links) {
            String href = link.absUrl("href");
            if (href.isEmpty() || href.contains("#")) continue;

            try {
                URL newUrl = new URL(href);
                if (!newUrl.getHost().equalsIgnoreCase(baseDomain)) continue;
            } catch (Exception ignored) {}

            if (scheduler.add(href, depth + 1)) {
                newLinks++;
            }
        }

        logger.debug("Dodano {} nowych linków z {}", newLinks, url);
    }

    @Override
    public void close() {
        logger.info("KafkaPageHandler zakończył pracę.");
    }
}
