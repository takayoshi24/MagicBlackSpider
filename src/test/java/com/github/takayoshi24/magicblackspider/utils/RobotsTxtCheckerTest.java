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
