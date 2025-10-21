package com.github.takayoshi24.magicblackspider.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Produkcyjny RobotsTxtChecker
 * - Cache'owanie robots.txt na host
 * - Obsługa User-agent
 * - Wsparcie dla Delay (Crawl-delay)
 * - Obsługa wielu hostów równocześnie
 */
public class RobotsTxtChecker {

    private static final Logger logger = LoggerFactory.getLogger(RobotsTxtChecker.class);

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
        return cache.computeIfAbsent(baseUrl, this::downloadAndParse);
    }

    private RobotsTxtRules downloadAndParse(String baseUrl) {
        RobotsTxtRules rules = new RobotsTxtRules();
        try {
            URL url = new URL(baseUrl + "/robots.txt");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
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
                    } else if (currentUserAgent != null && (currentUserAgent.equals("*") || currentUserAgent.equalsIgnoreCase(userAgent))) {
                        if (line.toLowerCase().startsWith("disallow:")) {
                            String path = line.split(":", 2)[1].trim();
                            rules.disallows.add(path);
                        } else if (line.toLowerCase().startsWith("allow:")) {
                            String path = line.split(":", 2)[1].trim();
                            rules.allows.add(path);
                        } else if (line.toLowerCase().startsWith("crawl-delay:")) {
                            try {
                                rules.crawlDelayMillis = (long) (Double.parseDouble(line.split(":", 2)[1].trim()) * 1000);
                            } catch (NumberFormatException e) {
                                logger.warn("Niepoprawny crawl-delay w robots.txt {}: {}", baseUrl, line);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Błąd pobierania robots.txt dla {}: {}", baseUrl, e.getMessage());
        }
        rules.lastFetched = Instant.now();
        return rules;
    }

    /**
     * Sprawdza, czy URL jest dozwolony
     */
    public boolean isAllowed(String url, RobotsTxtRules rules, String baseUrl) {
        try {
            String path = new URL(url).getPath();
            // Allow ma wyższy priorytet niż Disallow
            for (String allow : rules.allows) {
                if (path.startsWith(allow)) return true;
            }
            for (String disallow : rules.disallows) {
                if (path.startsWith(disallow)) return false;
            }
        } catch (Exception e) {
            logger.warn("Błąd parsowania URL {}: {}", url, e.getMessage());
        }
        return true; // domyślnie pozwól jeśli brak reguł
    }

    /**
     * Klasa przechowująca reguły robots.txt dla hosta
     */
    public static class RobotsTxtRules {
        public final Set<String> disallows = new HashSet<>();
        public final Set<String> allows = new HashSet<>();
        public long crawlDelayMillis = 0;
        public Instant lastFetched = Instant.now();
    }
}
