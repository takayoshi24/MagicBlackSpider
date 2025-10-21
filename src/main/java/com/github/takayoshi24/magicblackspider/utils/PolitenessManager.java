package com.github.takayoshi24.magicblackspider.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Produkcyjny PolitenessManager
 * - Zapewnia minimalny odstęp między żądaniami do tego samego hosta
 * - Uwzględnia crawl-delay z robots.txt
 * - Thread-safe
 */
public class PolitenessManager {

    private static final Logger logger = LoggerFactory.getLogger(PolitenessManager.class);

    private final long defaultDelayMillis;
    private final Map<String, Instant> lastAccessMap = new ConcurrentHashMap<>();

    public PolitenessManager(long defaultDelayMillis) {
        this.defaultDelayMillis = defaultDelayMillis;
    }

    /**
     * Upewnia się, że żądanie do danego hosta jest "grzeczne"
     * @param url URL do pobrania
     * @param crawlDelayMillis opcjonalny crawl-delay z robots.txt, jeśli 0 użyje defaultDelay
     */
    public void ensurePolite(String url, long crawlDelayMillis) {
        try {
            String host = extractHost(url);
            long delay = crawlDelayMillis > 0 ? crawlDelayMillis : defaultDelayMillis;

            while (true) {
                Instant last = lastAccessMap.get(host);
                Instant now = Instant.now();
                if (last == null || now.toEpochMilli() - last.toEpochMilli() >= delay) {
                    lastAccessMap.put(host, now);
                    break;
                } else {
                    long sleepTime = delay - (now.toEpochMilli() - last.toEpochMilli());
                    logger.debug("PolitenessManager: czekam {} ms dla hosta {}", sleepTime, host);
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                }
            }
        } catch (Exception e) {
            logger.warn("PolitenessManager: błąd przy url {}: {}", url, e.getMessage());
        }
    }

    /**
     * Domyślna wersja bez crawl-delay
     */
    public void ensurePolite(String url) {
        ensurePolite(url, 0);
    }

    private String extractHost(String url) throws Exception {
        return new java.net.URL(url).getHost();
    }

    /**
     * Resetuje statystyki last access (do testów lub restartu crawl'a)
     */
    public void reset() {
        lastAccessMap.clear();
    }
}
