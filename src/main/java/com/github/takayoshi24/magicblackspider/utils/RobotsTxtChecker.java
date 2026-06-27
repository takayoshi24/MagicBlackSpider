package com.github.takayoshi24.magicblackspider.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Produkcyjny RobotsTxtChecker dla MagicBlackSpider:
 * - Cache'owanie robots.txt na host
 * - Obsługa User-agent
 * - Crawl-delay
 * - Thread-safe, obsługa wielu hostów
 */
public class RobotsTxtChecker {

    private static final Logger logger = LoggerFactory.getLogger(RobotsTxtChecker.class);

    static final Duration CACHE_TTL = Duration.ofMinutes(30);
    static final long MAX_CRAWL_DELAY_MILLIS = 60_000L;

    private final Map<String, RobotsTxtRules> cache = new ConcurrentHashMap<>();
    private final String userAgent;

    public RobotsTxtChecker() {
        this.userAgent = "MagicBlackSpider";
    }

    public RobotsTxtChecker(String userAgent) {
        this.userAgent = userAgent;
    }

    /**
     * Pobiera i parsuje robots.txt dla hosta
     */
    public RobotsTxtRules fetchRules(String baseUrl) {
        return cache.compute(baseUrl, (key, existing) -> {
            if (existing != null && Duration.between(existing.lastFetched, Instant.now()).compareTo(CACHE_TTL) < 0) {
                return existing;
            }
            return downloadAndParse(key);
        });
    }

    private RobotsTxtRules downloadAndParse(String baseUrl) {
        RobotsTxtRules rules = new RobotsTxtRules();
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) URI.create(baseUrl + "/robots.txt").toURL().openConnection();
            conn.setRequestProperty("User-Agent", userAgent);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            int code = conn.getResponseCode();
            if (code != 200) {
                logger.info("Brak robots.txt lub błąd HTTP {} dla {}", code, baseUrl);
                return rules;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                String currentUserAgent = null;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) continue;

                    if (line.toLowerCase().startsWith("user-agent:")) {
                        currentUserAgent = line.split(":", 2)[1].trim();
                    } else if (currentUserAgent != null &&
                            (currentUserAgent.equals("*") || currentUserAgent.equalsIgnoreCase(userAgent))) {
                        if (line.toLowerCase().startsWith("disallow:")) {
                            String path = line.split(":", 2)[1].trim();
                            if (!path.isEmpty()) {
                                rules.disallows.add(path);
                            }
                        } else if (line.toLowerCase().startsWith("allow:")) {
                            String path = line.split(":", 2)[1].trim();
                            if (!path.isEmpty()) {
                                rules.allows.add(path);
                            }
                        } else if (line.toLowerCase().startsWith("crawl-delay:")) {
                            try {
                                long parsed = (long)(Double.parseDouble(line.split(":", 2)[1].trim()) * 1000);
                                if (parsed > MAX_CRAWL_DELAY_MILLIS) {
                                    logger.warn("Crawl-Delay {}ms from {} exceeds cap of {}ms; clamping",
                                            parsed, baseUrl, MAX_CRAWL_DELAY_MILLIS);
                                    parsed = MAX_CRAWL_DELAY_MILLIS;
                                }
                                rules.crawlDelayMillis = parsed;
                            } catch (NumberFormatException e) {
                                logger.warn("Niepoprawny crawl-delay w robots.txt {}: {}", baseUrl, line);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Błąd pobierania robots.txt dla {}: {}", baseUrl, e.getMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        rules.lastFetched = Instant.now();
        return rules;
    }

    /**
     * Sprawdza, czy URL jest dozwolony (wygodne przeciążenie dla crawlera)
     */
    public boolean isAllowed(String url) {
        try {
            URI u = URI.create(url);
            String baseUrl = u.getScheme() + "://" + u.getAuthority();
            RobotsTxtRules rules = fetchRules(baseUrl);
            return isAllowed(url, rules, baseUrl);
        } catch (Exception e) {
            logger.warn("Niepoprawny URL {}: {}", url, e.getMessage());
            return false;
        }
    }

    /**
     * Sprawdza URL względem reguł dla hosta
     */
    public boolean isAllowed(String url, RobotsTxtRules rules, String baseUrl) {
        try {
            String path = URI.create(url).getPath();
            // Most specific (longest) matching rule wins; ties go to Allow
            String bestAllow = rules.allows.stream().filter(path::startsWith).findFirst().orElse(null);
            String bestDisallow = rules.disallows.stream().filter(path::startsWith).findFirst().orElse(null);
            if (bestAllow == null && bestDisallow == null) return true;
            if (bestAllow == null) return false;
            if (bestDisallow == null) return true;
            return bestAllow.length() >= bestDisallow.length();
        } catch (Exception e) {
            logger.warn("Malformed URL — denying crawl {}: {}", url, e.getMessage());
            return false;
        }
    }

    /**
     * Klasa przechowująca reguły robots.txt dla hosta
     */
    public static class RobotsTxtRules {
        private static final Comparator<String> BY_LENGTH_DESC =
                Comparator.comparingInt(String::length).reversed().thenComparing(Comparator.naturalOrder());
        public final Set<String> disallows = new TreeSet<>(BY_LENGTH_DESC);
        public final Set<String> allows = new TreeSet<>(BY_LENGTH_DESC);
        public long crawlDelayMillis = 0;
        public Instant lastFetched = Instant.now();
    }
}
