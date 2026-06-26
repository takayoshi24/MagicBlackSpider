package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.Fetcher;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import com.github.takayoshi24.magicblackspider.utils.PolitenessManager;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies fix for issue #9: worker threads must not block during politeness delays.
 */
class MagicBlackSpiderPolitenessDispatchTest {

    /**
     * Verifies the requeue path: a URL that cannot be dispatched immediately due to
     * politeness delay is re-queued by the dispatch loop and eventually processed.
     */
    @Test
    void requeuedUrl_isProcessedAfterDelayElapses() throws InterruptedException {
        long politenessMs = 50;

        Scheduler scheduler = new Scheduler();
        scheduler.add("http://example.com/page/1", 0);
        scheduler.add("http://example.com/page/2", 0);

        Fetcher stubFetcher = url -> Jsoup.parse("<html></html>", url);
        PageHandler noopHandler = new PageHandler() {
            @Override public void handle(Page page, Scheduler sched, int depth) {}
            @Override public void close() {}
        };

        PolitenessManager pm = new PolitenessManager(politenessMs);
        RobotsTxtChecker permissive = new RobotsTxtChecker() {
            @Override public RobotsTxtRules fetchRules(String baseUrl) { return new RobotsTxtRules(); }
        };

        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler, stubFetcher, noopHandler, 2, politenessMs, pm, permissive);

        spider.start("http://example.com/seed", 2);

        // Both pages must be processed despite the second same-host URL needing a requeue.
        assertEquals(2, spider.getProcessedCount());
    }

    @Test
    void tryAcquire_claimsSlotAndReturnsZero_whenDelayElapsed() {
        PolitenessManager pm = new PolitenessManager(100);
        assertEquals(0, pm.tryAcquire("http://example.com/page/1", 0));
    }

    @Test
    void tryAcquire_returnsPositiveWaitTime_whenDelayNotElapsed() {
        PolitenessManager pm = new PolitenessManager(5000);
        pm.tryAcquire("http://example.com/page/1", 0); // claim slot
        long wait = pm.tryAcquire("http://example.com/page/2", 0); // same host, not ready
        assertTrue(wait > 0, "Expected a positive wait time, got " + wait);
    }

    @Test
    void scheduler_requeue_doesNotDedup() {
        Scheduler scheduler = new Scheduler();
        Scheduler.UrlWithDepth url = new Scheduler.UrlWithDepth("http://example.com/", 0);
        scheduler.add(url.url, url.depth);
        scheduler.requeue(url);
        assertEquals(2, scheduler.queueSize());
    }
}
