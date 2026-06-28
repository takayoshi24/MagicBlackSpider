package com.github.takayoshi24.magicblackspider.fetcher;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * Produkcyjny SimpleFetcher
 * - Obsługa timeoutów, retry i User-Agent
 * - Thread-safe
 * - Wsparcie dla limitu pobrań
 */
public class SimpleFetcher implements Fetcher {

    private static final Logger logger = LoggerFactory.getLogger(SimpleFetcher.class);
    private static final int MAX_REDIRECTS = 5;

    private final int timeoutMillis;
    private final int maxRetries;
    private final String userAgent;
    private final long retryDelayMillis;

    public SimpleFetcher(int timeoutMillis) {
        this(timeoutMillis, 3, "MagicBlackSpider", 1000L);
    }

    public SimpleFetcher(int timeoutMillis, int maxRetries, String userAgent) {
        this(timeoutMillis, maxRetries, userAgent, 1000L);
    }

    public SimpleFetcher(int timeoutMillis, int maxRetries, String userAgent, long retryDelayMillis) {
        this.timeoutMillis = timeoutMillis;
        this.maxRetries = maxRetries;
        this.userAgent = userAgent;
        this.retryDelayMillis = retryDelayMillis;
    }

    @Override
    public Document fetch(String url) throws IOException {
        if (isBlockedUrl(url)) {
            logger.warn("[SSRF] Blocked fetch attempt to private/loopback address: {}", url);
            throw new IOException("Blocked URL (private/loopback address): " + url);
        }
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                logger.debug("Fetching URL (attempt {}): {}", attempt + 1, url);
                return fetchWithRedirects(url);
            } catch (IOException e) {
                attempt++;
                logger.warn("Błąd pobierania URL {} ({}), próba {}/{}", url, e.getMessage(), attempt, maxRetries);
                if (attempt < maxRetries && retryDelayMillis > 0) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(retryDelayMillis);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during fetch retry", ex);
                    }
                }
            }
        }
        throw new IOException("Nie udało się pobrać URL po " + maxRetries + " próbach: " + url);
    }

    private Document fetchWithRedirects(String url) throws IOException {
        String current = url;
        for (int redirects = 0; redirects <= MAX_REDIRECTS; redirects++) {
            Connection.Response response = Jsoup.connect(current)
                    .userAgent(userAgent)
                    .timeout(timeoutMillis)
                    .followRedirects(false)
                    .execute();
            int status = response.statusCode();
            if (status >= 300 && status < 400) {
                String location = response.header("Location");
                if (location == null || location.isBlank()) {
                    throw new IOException("Redirect without Location header from: " + current);
                }
                try {
                    location = new URI(current).resolve(location.trim()).toString();
                } catch (URISyntaxException e) {
                    throw new IOException("Malformed redirect Location: " + location, e);
                }
                if (isBlockedUrl(location)) {
                    throw new IOException("Redirect to blocked address rejected: " + location);
                }
                logger.debug("Following redirect {} -> {}", current, location);
                current = location;
                continue;
            }
            if (status >= 500) {
                throw new IOException("HTTP " + status + " for URL: " + current);
            }
            if (status >= 400) {
                logger.warn("HTTP {} for URL: {}", status, current);
                return null;
            }
            return response.parse();
        }
        throw new IOException("Too many redirects for URL: " + url);
    }

    /**
     * Returns true if the URL's resolved host is a loopback, link-local, or private (RFC-1918) address.
     * Treats unresolvable or malformed URLs as blocked.
     */
    public static boolean isBlockedUrl(String url) {
        try {
            String host = new URI(url).getHost();
            if (host == null) return true;
            for (InetAddress addr : InetAddress.getAllByName(host)) {
                if (isPrivateAddress(addr)) return true;
            }
            return false;
        } catch (URISyntaxException | UnknownHostException e) {
            return true;
        }
    }

    private static boolean isPrivateAddress(InetAddress addr) {
        return addr.isLoopbackAddress()
                || addr.isLinkLocalAddress()
                || addr.isSiteLocalAddress()
                || addr.isAnyLocalAddress();
    }
}
