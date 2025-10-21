package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.Fetcher;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import com.github.takayoshi24.magicblackspider.utils.PolitenessManager;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker.RobotsTxtRules;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Główny crawler MagicBlackSpider
 */
public class MagicBlackSpider {

    private static final Logger logger = LoggerFactory.getLogger(MagicBlackSpider.class);

    private final Scheduler scheduler;
    private final Fetcher fetcher;
    private final PageHandler handler;
    private final ExecutorService executor;
    private final long politenessMillis;

    private final RobotsTxtChecker robotsChecker;
    private final PolitenessManager politenessManager;

    public MagicBlackSpider(Scheduler scheduler, Fetcher fetcher, PageHandler handler, int threads, long politenessMillis) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.handler = handler;
        this.executor = Executors.newFixedThreadPool(threads);
        this.politenessMillis = politenessMillis;
        this.robotsChecker = new RobotsTxtChecker();
        this.politenessManager = new PolitenessManager(politenessMillis);
    }

    public void start(String seedUrl, int maxPages) {
        scheduler.add(seedUrl);
        int processed = 0;

        while (!scheduler.isEmpty() && processed < maxPages) {
            String url = scheduler.next();
            if (url == null) continue;

            executor.submit(() -> {
                try {
                    String baseUrl = new java.net.URL(url).getProtocol() + "://" + new java.net.URL(url).getHost();
                    RobotsTxtRules rules = robotsChecker.fetchRules(baseUrl);

                    if (!robotsChecker.isAllowed(url, rules, baseUrl)) {
                        logger.info("Odrzucono URL z robots.txt: {}", url);
                        return;
                    }

                    politenessManager.ensurePolite(url, rules.crawlDelayMillis);

                    Document doc = fetcher.fetch(url);
                    handler.handle(new Page(url, doc), scheduler);
                    scheduler.markVisited(url);
                } catch (Exception e) {
                    logger.warn("Błąd podczas przetwarzania URL {}: {}", url, e.getMessage());
                }

            });

            processed++;
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
