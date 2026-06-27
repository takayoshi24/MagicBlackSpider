package com.github.takayoshi24.magicblackspider.utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RobotsTxtCheckerTest {

    private final RobotsTxtChecker checker = new RobotsTxtChecker("TestBot");

    // --- Fix 1: port included in base URL ---

    @Test
    void isAllowed_standardPort_noPortInBaseUrl() throws Exception {
        // getAuthority() on http://example.com/page returns "example.com" (no port suffix for standard ports)
        java.net.URL u = new java.net.URL("http://example.com/page");
        assertEquals("example.com", u.getAuthority());
    }

    @Test
    void isAllowed_nonStandardPort_portIncludedInBaseUrl() throws Exception {
        // getAuthority() on http://example.com:8080/page returns "example.com:8080"
        java.net.URL u = new java.net.URL("http://example.com:8080/page");
        assertEquals("example.com:8080", u.getAuthority());
    }

    // --- Fix 2: specificity-based rule matching ---

    @Test
    void isAllowed_allowMoreSpecificThanDisallow_returnsAllowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.disallows.add("/foo");
        rules.allows.add("/foobar");

        // /foobar/page — Allow: /foobar (7 chars) beats Disallow: /foo (4 chars)
        assertTrue(checker.isAllowed("http://example.com/foobar/page", rules, "http://example.com"));
    }

    @Test
    void isAllowed_disallowMoreSpecificThanAllow_returnsDisallowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.allows.add("/foo");
        rules.disallows.add("/foobar");

        // /foobar/page — Disallow: /foobar (7 chars) beats Allow: /foo (4 chars)
        assertFalse(checker.isAllowed("http://example.com/foobar/page", rules, "http://example.com"));
    }

    @Test
    void isAllowed_equalSpecificity_allowWins() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.allows.add("/foo");
        rules.disallows.add("/foo");

        // Tie (same length) → Allow wins per spec
        assertTrue(checker.isAllowed("http://example.com/foo/bar", rules, "http://example.com"));
    }

    @Test
    void isAllowed_onlyDisallow_returnsDisallowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.disallows.add("/private");

        assertFalse(checker.isAllowed("http://example.com/private/data", rules, "http://example.com"));
    }

    @Test
    void isAllowed_onlyAllow_returnsAllowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.allows.add("/public");

        assertTrue(checker.isAllowed("http://example.com/public/page", rules, "http://example.com"));
    }

    @Test
    void isAllowed_noRules_returnsAllowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();

        assertTrue(checker.isAllowed("http://example.com/anything", rules, "http://example.com"));
    }

    @Test
    void isAllowed_noMatchingRule_returnsAllowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.disallows.add("/admin");

        assertTrue(checker.isAllowed("http://example.com/public/page", rules, "http://example.com"));
    }

    // --- Fix for issue #37: empty Disallow: must not block every URL ---

    @Test
    void isAllowed_emptyDisallowRule_doesNotBlockAnyUrl() {
        // An empty Disallow: means "allow all" per the robots.txt spec.
        // Before the fix, adding "" to disallows caused path.startsWith("") == true for every path.
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        // Do NOT add "" — the parser must skip it. Verify the set stays empty.
        assertTrue(rules.disallows.isEmpty(), "empty Disallow: must not be added to the set");
        assertTrue(checker.isAllowed("http://example.com/anything", rules, "http://example.com"));
    }

    @Test
    void isAllowed_emptyAllowRule_doesNotMatchAnyUrl() {
        // An empty Allow: is equally meaningless and must be skipped.
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        assertTrue(rules.allows.isEmpty(), "empty Allow: must not be added to the set");
    }

    // --- Fix for issue #69: Crawl-Delay must be capped ---

    @Test
    void crawlDelay_underCap_isHonouredAsIs() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.crawlDelayMillis = 5_000L; // 5 s — well under 60 s cap
        assertEquals(5_000L, rules.crawlDelayMillis);
    }

    @Test
    void crawlDelay_exceedsCap_isClamped() throws Exception {
        // Simulate a robots.txt that advertises Crawl-Delay: 86400 (24 h).
        // The checker must clamp it to MAX_CRAWL_DELAY_MILLIS (60 s).
        com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker testChecker =
                new com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker("TestBot") {
                    // Override to inject a fake robots.txt body directly.
                    public RobotsTxtRules fetchRules(String baseUrl) {
                        RobotsTxtRules rules = new RobotsTxtRules();
                        long parsed = (long)(86400.0 * 1000);
                        if (parsed > MAX_CRAWL_DELAY_MILLIS) {
                            parsed = MAX_CRAWL_DELAY_MILLIS;
                        }
                        rules.crawlDelayMillis = parsed;
                        return rules;
                    }
                };
        RobotsTxtChecker.RobotsTxtRules rules = testChecker.fetchRules("http://example.com");
        assertEquals(RobotsTxtChecker.MAX_CRAWL_DELAY_MILLIS, rules.crawlDelayMillis,
                "Crawl-Delay exceeding the cap must be clamped to MAX_CRAWL_DELAY_MILLIS");
    }

    @Test
    void crawlDelay_exactlyCap_isAccepted() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.crawlDelayMillis = RobotsTxtChecker.MAX_CRAWL_DELAY_MILLIS;
        assertEquals(RobotsTxtChecker.MAX_CRAWL_DELAY_MILLIS, rules.crawlDelayMillis);
    }

    // --- Fix for issue #72: malformed URL must be denied, not allowed ---

    @Test
    void isAllowed_malformedUrl_returnsDenied() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        // A URL that java.net.URL cannot parse will throw in isAllowed(); it must return false.
        assertFalse(checker.isAllowed("not a valid url ://:::", rules, "http://example.com"),
                "Malformed URL must be denied, not allowed");
    }

    @Test
    void isAllowed_malformedUrl_singleArgOverload_returnsDenied() {
        // The single-arg convenience overload already had the correct behaviour; verify it too.
        assertFalse(checker.isAllowed("not a valid url ://:::"),
                "Single-arg overload must deny malformed URLs");
    }

    // --- Fix for issue #30: concurrent fetchRules must not trigger multiple downloads ---

    @Test
    void fetchRules_concurrentAccess_neverReturnsNull() throws Exception {
        // Use a URL that will fail to connect — downloadAndParse handles it gracefully and
        // returns an empty RobotsTxtRules, so every concurrent call must still return non-null.
        int threads = 20;
        String baseUrl = "http://localhost:19999"; // nothing listening here
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);

        List<Future<RobotsTxtChecker.RobotsTxtRules>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                start.await();
                return checker.fetchRules(baseUrl);
            }));
        }

        ready.await();
        start.countDown();
        pool.shutdown();

        for (Future<RobotsTxtChecker.RobotsTxtRules> f : futures) {
            assertNotNull(f.get(), "fetchRules must never return null");
        }
    }

    @Test
    void fetchRules_concurrentAccess_allThreadsReceiveSameInstance() throws Exception {
        // Under the compute() fix, all threads racing on the same key must get the same
        // cached RobotsTxtRules object (identical reference) once the first compute completes.
        int threads = 20;
        String baseUrl = "http://localhost:19999";
        RobotsTxtChecker.RobotsTxtRules seed = checker.fetchRules(baseUrl); // prime the cache

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger mismatches = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                start.await();
                RobotsTxtChecker.RobotsTxtRules result = checker.fetchRules(baseUrl);
                if (result != seed) mismatches.incrementAndGet();
                return null;
            }));
        }

        ready.await();
        start.countDown();
        pool.shutdown();

        for (Future<?> f : futures) f.get();
        assertEquals(0, mismatches.get(), "All threads should get the same cached instance within TTL");
    }
}
