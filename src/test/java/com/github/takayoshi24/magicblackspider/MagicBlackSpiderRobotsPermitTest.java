package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.Fetcher;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import com.github.takayoshi24.magicblackspider.utils.PolitenessManager;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MagicBlackSpiderRobotsPermitTest {

    /**
     * Verifies that URLs blocked by robots.txt release their page permit so the
     * full maxPages budget is available for real pages.
     *
     * Setup:
     *   maxPages = 4
     *   Queue (FIFO): blocked1, blocked2, page1..page4, then seed (added by start())
     *
     * With the fix the two blocked URLs release their permits, all 4 slots are
     * consumed by page1–page4, and the loop exits when permits reach zero.
     * Without the fix the two blocked URLs each silently burn a permit, leaving
     * only 2 slots for real pages — processed count would be 2, not 4.
     */
    @Test
    @Timeout(10)
    void disallowedUrls_doNotConsumePagePermits() throws InterruptedException {
        int maxPages = 4;

        Scheduler scheduler = new Scheduler();
        // Queue blocked URLs first so they are dispatched before the allowed ones.
        scheduler.add("http://example.com/blocked1", 0);
        scheduler.add("http://example.com/blocked2", 0);
        for (int i = 1; i <= maxPages; i++) {
            scheduler.add("http://example.com/page" + i, 0);
        }
        // seed is added inside start() and lands at the back — it should never
        // run because all 4 permits are consumed by page1–page4.

        AtomicInteger processedCount = new AtomicInteger(0);

        Fetcher stubFetcher = url -> Jsoup.parse("<html><body></body></html>", url);
        PageHandler countingHandler = new PageHandler() {
            @Override public void handle(Page page, Scheduler sched, int depth) {
                processedCount.incrementAndGet();
            }
            @Override public void close() {}
        };

        PolitenessManager zeroPoliteness = new PolitenessManager(0);
        RobotsTxtChecker blockingRobots = new RobotsTxtChecker() {
            @Override
            public RobotsTxtRules fetchRules(String baseUrl) {
                return new RobotsTxtRules();
            }

            @Override
            public boolean isAllowed(String url, RobotsTxtRules rules, String baseUrl) {
                return !url.contains("/blocked");
            }
        };

        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler, stubFetcher, countingHandler, 2, 0L,
                zeroPoliteness, blockingRobots);

        spider.start("http://example.com/seed", maxPages);

        assertEquals(maxPages, processedCount.get(),
                "Expected " + maxPages + " pages processed but got " + processedCount.get()
                        + " — blocked URLs likely leaked a permit each");
    }

    /**
     * Verifies that the base URL passed to fetchRules() includes the port for
     * non-standard-port seed URLs (fix for issue #38).
     *
     * Before the fix, getHost() was used and "http://example.com:8080/seed"
     * produced baseUrl "http://example.com" — robots.txt fetched from the wrong port.
     * After the fix, getAuthority() is used and the baseUrl is "http://example.com:8080".
     */
    @Test
    @Timeout(10)
    void nonStandardPort_baseUrlIncludesPort() throws InterruptedException {
        AtomicReference<String> capturedBaseUrl = new AtomicReference<>();

        Scheduler scheduler = new Scheduler();
        Fetcher stubFetcher = url -> Jsoup.parse("<html><body></body></html>", url);
        PageHandler noOpHandler = new PageHandler() {
            @Override public void handle(Page page, Scheduler sched, int depth) {}
            @Override public void close() {}
        };

        PolitenessManager zeroPoliteness = new PolitenessManager(0);
        RobotsTxtChecker capturingRobots = new RobotsTxtChecker() {
            @Override
            public RobotsTxtRules fetchRules(String baseUrl) {
                capturedBaseUrl.set(baseUrl);
                return new RobotsTxtRules();
            }
        };

        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler, stubFetcher, noOpHandler, 1, 0L,
                zeroPoliteness, capturingRobots);

        spider.start("http://example.com:8080/seed", 1);

        assertTrue(capturedBaseUrl.get() != null && capturedBaseUrl.get().contains(":8080"),
                "fetchRules() must receive a baseUrl with the port, got: " + capturedBaseUrl.get());
    }
}
