package com.github.takayoshi24.magicblackspider.fetcher;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that retryDelayMillis=0 skips all sleeps, keeping test execution fast,
 * and that exhausting retries throws IOException.
 */
class SimpleFetcherRetryDelayTest {

    @Test
    void retriesExhausted_throwsIOException_withoutBlocking() {
        // retryDelayMillis=0 — no Thread.sleep, so this completes immediately even with retries
        SimpleFetcher fetcher = new SimpleFetcher(1, 2, "TestAgent", 0L);

        long start = System.currentTimeMillis();
        assertThrows(IOException.class, () -> fetcher.fetch("http://localhost:1"));
        long elapsed = System.currentTimeMillis() - start;

        // With retryDelayMillis=0 there should be no multi-second stall
        assertTrue(elapsed < 5000, "Retries with delay=0 should not stall threads; elapsed=" + elapsed + "ms");
    }

    @Test
    void zeroDelayConstructor_doesNotSleepBetweenRetries() throws Exception {
        SimpleFetcher fetcher = new SimpleFetcher(1, 3, "TestAgent", 0L);
        long start = System.currentTimeMillis();
        try {
            fetcher.fetch("http://localhost:1");
        } catch (IOException ignored) {
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 5000, "No sleep between retries expected; elapsed=" + elapsed + "ms");
    }
}
