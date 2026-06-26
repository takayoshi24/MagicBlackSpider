package com.github.takayoshi24.magicblackspider.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
     * Non-blocking check: atomically claims the host slot if the delay has elapsed.
     * @return 0 if the slot was claimed (caller may proceed), or remaining wait millis otherwise.
     */
    public long tryAcquire(String url, long crawlDelayMillis) {
        try {
            String host = extractHost(url);
            long delay = crawlDelayMillis > 0 ? crawlDelayMillis : defaultDelayMillis;
            long[] waitTime = {0};
            lastAccessMap.compute(host, (h, last) -> {
                Instant now = Instant.now();
                if (last == null || now.toEpochMilli() - last.toEpochMilli() >= delay) {
                    waitTime[0] = 0;
                    return now;
                }
                waitTime[0] = delay - (now.toEpochMilli() - last.toEpochMilli());
                return last;
            });
            return waitTime[0];
        } catch (Exception e) {
            logger.warn("PolitenessManager: error for url {}: {}", url, e.getMessage());
            return 0;
        }
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
