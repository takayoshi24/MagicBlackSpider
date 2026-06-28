package com.github.takayoshi24.magicblackspider.fetcher;

import com.sun.net.httpserver.HttpServer;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies 5xx retry and 4xx fail-fast behaviour in SimpleFetcher.
 *
 * Uses a JDK HttpServer bound to localhost. SSRF protection is bypassed via a
 * package-private subclass that overrides isSsrfBlocked() — production callers
 * always go through the real isBlockedUrl() guard.
 */
class SimpleFetcher5xxRetryTest {

    /** Subclass that allows localhost connections for in-process test servers. */
    private static class LocalFetcher extends SimpleFetcher {
        LocalFetcher(int timeoutMillis, int maxRetries, long retryDelayMillis) {
            super(timeoutMillis, maxRetries, "TestAgent", retryDelayMillis);
        }

        @Override
        protected boolean isSsrfBlocked(String url) {
            return false;
        }
    }

    private HttpServer server;
    private int port;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        port = server.getAddress().getPort();
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void fetch_retriesOn503_andSucceedsOnSecondAttempt() throws IOException {
        AtomicInteger callCount = new AtomicInteger(0);
        server.createContext("/page", exchange -> {
            int call = callCount.incrementAndGet();
            byte[] body;
            int status;
            if (call == 1) {
                status = 503;
                body = "Service Unavailable".getBytes(StandardCharsets.UTF_8);
            } else {
                status = 200;
                body = "<html><body>ok</body></html>".getBytes(StandardCharsets.UTF_8);
            }
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        LocalFetcher fetcher = new LocalFetcher(5000, 3, 0L);
        Document doc = fetcher.fetch("http://localhost:" + port + "/page");

        assertNotNull(doc, "fetch() should succeed after retrying past the 503");
        assertEquals(2, callCount.get(), "Expected exactly 2 requests: 1×503 then 1×200");
    }

    @Test
    void fetch_throwsIOException_whenAll5xxRetriesExhausted() {
        server.createContext("/always503", exchange -> {
            byte[] body = "Service Unavailable".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        LocalFetcher fetcher = new LocalFetcher(5000, 2, 0L);
        assertThrows(IOException.class,
                () -> fetcher.fetch("http://localhost:" + port + "/always503"),
                "fetch() must throw IOException when all retries return 5xx");
    }

    @Test
    void fetch_returnsNull_for404_withoutRetrying() throws IOException {
        AtomicInteger callCount = new AtomicInteger(0);
        server.createContext("/missing", exchange -> {
            callCount.incrementAndGet();
            byte[] body = "Not Found".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(404, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        LocalFetcher fetcher = new LocalFetcher(5000, 3, 0L);
        Document doc = fetcher.fetch("http://localhost:" + port + "/missing");

        assertNull(doc, "fetch() must return null for 404 without retrying");
        assertEquals(1, callCount.get(), "404 must not trigger retries");
    }
}
