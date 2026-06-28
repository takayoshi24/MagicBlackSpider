package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.Fetcher;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import com.github.takayoshi24.magicblackspider.utils.PolitenessManager;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker;
import org.jsoup.Jsoup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class MagicBlackSpiderFailedCountTest {

    private static PolitenessManager zeroPoliteness() { return new PolitenessManager(0); }

    private static RobotsTxtChecker permissiveRobots() {
        return new RobotsTxtChecker() {
            @Override public RobotsTxtRules fetchRules(String baseUrl) { return new RobotsTxtRules(); }
        };
    }

    private static PageHandler noopHandler() {
        return new PageHandler() {
            @Override public void handle(Page page, Scheduler sched, int depth) {}
            @Override public void close() {}
        };
    }

    @Test
    void fetcherThrowsIOException_incrementsFailedCount() {
        Scheduler scheduler = new Scheduler();
        Fetcher throwingFetcher = url -> { throw new IOException("connection refused"); };

        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler, throwingFetcher, noopHandler(), 1, 0L,
                zeroPoliteness(), permissiveRobots());

        assertTimeoutPreemptively(Duration.ofSeconds(5),
                () -> spider.start("http://example.com/page1", 1));

        assertEquals(1, spider.getFailedCount(),
                "IOException from fetcher must increment failedCount");
        assertEquals(0, spider.getProcessedCount(),
                "No pages should count as processed after a fetch IOException");
    }

    @Test
    void fetcherReturnsNull_incrementsFailedCount() {
        Scheduler scheduler = new Scheduler();
        Fetcher nullFetcher = url -> null;

        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler, nullFetcher, noopHandler(), 1, 0L,
                zeroPoliteness(), permissiveRobots());

        assertTimeoutPreemptively(Duration.ofSeconds(5),
                () -> spider.start("http://example.com/page1", 1));

        assertEquals(1, spider.getFailedCount(),
                "null return from fetcher must increment failedCount");
        assertEquals(0, spider.getProcessedCount());
    }

    @Test
    void handlerException_doesNotIncrementFailedCount() {
        Scheduler scheduler = new Scheduler();
        Fetcher okFetcher = url -> Jsoup.parse("<html></html>", url);
        PageHandler throwingHandler = new PageHandler() {
            @Override public void handle(Page page, Scheduler sched, int depth) {
                throw new RuntimeException("handler error");
            }
            @Override public void close() {}
        };

        MagicBlackSpider spider = new MagicBlackSpider(
                scheduler, okFetcher, throwingHandler, 1, 0L,
                zeroPoliteness(), permissiveRobots());

        assertTimeoutPreemptively(Duration.ofSeconds(5),
                () -> spider.start("http://example.com/page1", 1));

        assertEquals(0, spider.getFailedCount(),
                "Exception thrown by the handler must NOT increment failedCount");
    }
}
