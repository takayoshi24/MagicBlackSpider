package com.github.takayoshi24.magicblackspider.handler;

import com.github.takayoshi24.magicblackspider.Page;
import com.github.takayoshi24.magicblackspider.Scheduler;
import org.jsoup.nodes.Element;


/**
 * Przykładowy handler: drukuje tytuł i dodaje linki do schedulera.
 */
import com.github.takayoshi24.magicblackspider.Page;
import com.github.takayoshi24.magicblackspider.Scheduler;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExampleHandler implements PageHandler {
    private static final Logger logger = LoggerFactory.getLogger(ExampleHandler.class);


    @Override
    public void handle(Page page, Scheduler scheduler) {
        try {
            logger.info("[PAGE] {} -> {}", page.getUrl(), page.getDocument().title());
            page.getDocument().select("a[href]").forEach((Element link) -> {
                String next = link.absUrl("href");
                if (next != null && !next.isEmpty()) scheduler.add(next);
            });
        } catch (Exception e) {
            logger.error("Błąd w handlerze dla: {}", page.getUrl(), e);
        }
    }
}