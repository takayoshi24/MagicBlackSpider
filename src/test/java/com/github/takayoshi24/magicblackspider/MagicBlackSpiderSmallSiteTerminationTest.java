package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.Fetcher;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import com.github.takayoshi24.magicblackspider.utils.PolitenessManager;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class MagicBlackSpiderSmallSiteTerminationTest {

    @Test
    void start_terminatesWhenSiteHasFewerPagesThanMaxPages() {
        int sitePages = 3;
        int maxPages = 1000;

        Scheduler scheduler = new Scheduler();
        for (int i = 1; i <= sitePages; i++) {
            scheduler.add("http://example.com/page/" + i, 0);
        }

        // Fetcher returns pages with no links so the scheduler is never refilled
        Fetcher stubFetcher = url -> Jsoup.parse("<html><body></body></html>", url);
        PageHandler noopHandler = new PageHandler() {
            @Override public void handle(Page page, Scheduler sched, int depth) {}
            @Override public void close() {}
        };

        PolitenessManager zeroPoliteness = new PolitenessManager(0);
        RobotsTxtChecker permissiveRobots = new RobotsTxtChecker() {
            @Override public RobotsTxtRules fetchRules(String baseUrl) { return new RobotsTxtRules(); }
        };

        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler, stubFetcher, noopHandler, 4, 0L,
                zeroPoliteness, permissiveRobots);

        // Seed is one of the pre-loaded URLs so it's deduped; total unique pages = sitePages
        assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                spider.start("http://example.com/page/1", maxPages),
                "Crawler hung when site had fewer pages than maxPages");

        assertEquals(sitePages, spider.getProcessedCount());
    }
}
