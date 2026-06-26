package com.github.takayoshi24.magicblackspider.fetcher;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Produkcyjny SimpleFetcher
 * - Obsługa timeoutów, retry i User-Agent
 * - Thread-safe
 * - Wsparcie dla limitu pobrań
 */
public class SimpleFetcher implements Fetcher {

    private static final Logger logger = LoggerFactory.getLogger(SimpleFetcher.class);

    private final int timeoutMillis;
    private final int maxRetries;
    private final String userAgent;

    public SimpleFetcher(int timeoutMillis) {
        this(timeoutMillis, 3, "MagicBlackSpider");
    }

    public SimpleFetcher(int timeoutMillis, int maxRetries, String userAgent) {
        this.timeoutMillis = timeoutMillis;
        this.maxRetries = maxRetries;
        this.userAgent = userAgent;
    }

    @Override
    public Document fetch(String url) throws IOException {
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                logger.debug("Fetching URL (attempt {}): {}", attempt + 1, url);
                Connection.Response response = Jsoup.connect(url)
                        .userAgent(userAgent)
                        .timeout(timeoutMillis)
                        .followRedirects(true)
                        .execute();
                if (response.statusCode() >= 400) {
                    logger.warn("HTTP {} for URL: {}", response.statusCode(), url);
                    return null;
                }
                return response.parse();
            } catch (IOException e) {
                attempt++;
                logger.warn("Błąd pobierania URL {} ({}), próba {}/{}", url, e.getMessage(), attempt, maxRetries);
                try {
                    TimeUnit.SECONDS.sleep(1); // krótkie opóźnienie między próbami
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during fetch retry", ex);
                }
            }
        }
        throw new IOException("Nie udało się pobrać URL po " + maxRetries + " próbach: " + url);
    }
}
