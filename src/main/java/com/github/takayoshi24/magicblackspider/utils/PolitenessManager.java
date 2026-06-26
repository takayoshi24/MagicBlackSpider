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
                boolean[] shouldProceed = {false};
                long[] sleepTime = {0};
                lastAccessMap.compute(host, (h, last) -> {
                    Instant now = Instant.now();
                    if (last == null || now.toEpochMilli() - last.toEpochMilli() >= delay) {
                        shouldProceed[0] = true;
                        return now;
                    }
                    sleepTime[0] = delay - (now.toEpochMilli() - last.toEpochMilli());
                    return last;
                });
                if (shouldProceed[0]) {
                    break;
                }
                logger.debug("PolitenessManager: czekam {} ms dla hosta {}", sleepTime[0], host);
                TimeUnit.MILLISECONDS.sleep(sleepTime[0]);
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
