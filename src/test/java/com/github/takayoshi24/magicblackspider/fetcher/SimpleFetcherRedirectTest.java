package com.github.takayoshi24.magicblackspider.fetcher;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SimpleFetcherRedirectTest {

    @Test
    void loopback_isBlocked() {
        assertTrue(SimpleFetcher.isBlockedUrl("http://127.0.0.1/secret"));
        assertTrue(SimpleFetcher.isBlockedUrl("http://localhost/admin"));
    }

    @Test
    void rfc1918_isBlocked() {
        assertTrue(SimpleFetcher.isBlockedUrl("http://10.0.0.1/"));
        assertTrue(SimpleFetcher.isBlockedUrl("http://172.16.0.1/"));
        assertTrue(SimpleFetcher.isBlockedUrl("http://192.168.1.1/"));
    }

    @Test
    void linkLocal_isBlocked() {
        // AWS metadata endpoint
        assertTrue(SimpleFetcher.isBlockedUrl("http://169.254.169.254/latest/meta-data/"));
    }

    @Test
    void malformedUrl_isBlocked() {
        assertTrue(SimpleFetcher.isBlockedUrl("not-a-url"));
        assertTrue(SimpleFetcher.isBlockedUrl("http://"));
    }

    @Test
    void publicUrl_isNotBlocked() {
        assertFalse(SimpleFetcher.isBlockedUrl("http://example.com/"));
        assertFalse(SimpleFetcher.isBlockedUrl("https://www.google.com/"));
    }
}
