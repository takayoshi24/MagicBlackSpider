package com.github.takayoshi24.magicblackspider.handler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HTMLKafkaServerSafeHrefTest {

    @Test
    void allowsHttpUrl() {
        assertEquals("http://example.com", HTMLKafkaServer.safeHref("http://example.com"));
    }

    @Test
    void allowsHttpsUrl() {
        assertEquals("https://example.com/path?q=1", HTMLKafkaServer.safeHref("https://example.com/path?q=1"));
    }

    @Test
    void blocksJavascriptUri() {
        assertEquals("#", HTMLKafkaServer.safeHref("javascript:alert(1)"));
    }

    @Test
    void blocksJavascriptUriMixedCase() {
        assertEquals("#", HTMLKafkaServer.safeHref("JavaScript:alert(1)"));
    }

    @Test
    void blocksDataUri() {
        assertEquals("#", HTMLKafkaServer.safeHref("data:text/html,<script>alert(1)</script>"));
    }

    @Test
    void blocksNullUrl() {
        assertEquals("#", HTMLKafkaServer.safeHref(null));
    }

    @Test
    void escapesSpecialCharsInHttpUrl() {
        assertEquals("https://example.com/&amp;path", HTMLKafkaServer.safeHref("https://example.com/&path"));
    }
}
