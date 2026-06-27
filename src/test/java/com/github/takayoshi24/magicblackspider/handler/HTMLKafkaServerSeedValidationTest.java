package com.github.takayoshi24.magicblackspider.handler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HTMLKafkaServerSeedValidationTest {

    @Test
    void acceptsNullUrl() {
        assertFalse(HTMLKafkaServer.isSeedUrlTooLong(null));
    }

    @Test
    void acceptsNormalUrl() {
        assertFalse(HTMLKafkaServer.isSeedUrlTooLong("https://example.com"));
    }

    @Test
    void acceptsUrlExactlyAtLimit() {
        String url = "https://example.com/" + "a".repeat(HTMLKafkaServer.MAX_SEED_URL_LENGTH - "https://example.com/".length());
        assertFalse(HTMLKafkaServer.isSeedUrlTooLong(url));
    }

    @Test
    void rejectsUrlOneCharOverLimit() {
        String url = "a".repeat(HTMLKafkaServer.MAX_SEED_URL_LENGTH + 1);
        assertTrue(HTMLKafkaServer.isSeedUrlTooLong(url));
    }

    @Test
    void rejectsMegaScaleUrl() {
        String url = "https://example.com/" + "a".repeat(1_000_000);
        assertTrue(HTMLKafkaServer.isSeedUrlTooLong(url));
    }
}
