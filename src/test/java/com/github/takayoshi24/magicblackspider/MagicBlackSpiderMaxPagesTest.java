package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.Fetcher;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import com.github.takayoshi24.magicblackspider.utils.PolitenessManager;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MagicBlackSpiderMaxPagesTest {

    @Test
    void start_doesNotExceedMaxPages_withConcurrentWorkers() throws InterruptedException {
        int maxPages = 5;
        int threads = 4;

        Scheduler scheduler = new Scheduler();
        // Pre-load more URLs than maxPages so the queue is never exhausted
        for (int i = 1; i <= 20; i++) {
            scheduler.add("http://example.com/page/" + i, 0);
        }

        Fetcher stubFetcher = url -> Jsoup.parse("<html><body></body></html>", url);
        PageHandler stubHandler = new PageHandler() {
            @Override public void handle(Page page, Scheduler sched, int depth) {}
            @Override public void close() {}
        };

        // Zero politeness delay and a RobotsTxtChecker that never makes network calls
        PolitenessManager zeroPoliteness = new PolitenessManager(0);
        RobotsTxtChecker permissiveRobots = new RobotsTxtChecker() {
            @Override
            public RobotsTxtRules fetchRules(String baseUrl) {
                return new RobotsTxtRules(); // empty rules → all allowed
            }
        };

        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler, stubFetcher, stubHandler, threads, 0L,
                zeroPoliteness, permissiveRobots);

        spider.start("http://example.com/page/0", maxPages);

        assertTrue(spider.getProcessedCount() <= maxPages,
                "Processed " + spider.getProcessedCount() + " pages but maxPages=" + maxPages);
    }
}
