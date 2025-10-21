package com.github.takayoshi24.magicblackspider;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Produkcyjny Scheduler dla MagicBlackSpider.
 * - Bez duplikatów (thread-safe)
 * - Nie blokuje się na 74 stronach
 * - Działa z wielowątkowym MagicBlackSpider
 */
public class Scheduler {

    private final Queue<String> queue = new ConcurrentLinkedQueue<>();
    private final Set<String> seen = ConcurrentHashMap.newKeySet(); // odwiedzone + w kolejce
    private volatile long totalAdded = 0;

    /**
     * Dodaje URL do kolejki (jeśli jeszcze nie był widziany).
     */
    public boolean add(String url) {
        if (url == null || url.isBlank()) return false;
        String normalized = normalize(url);
        if (seen.add(normalized)) {
            queue.offer(normalized);
            totalAdded++;
            return true;
        }
        return false;
    }

    /**
     * Oznacza URL jako odwiedzony (dla statystyk)
     */
    public void markVisited(String url) {
        // nic nie trzeba robić – jest już w `seen`
    }

    /**
     * Pobiera następny URL do przetworzenia
     */
    public String next() {
        return queue.poll();
    }

    /**
     * Czy kolejka jest pusta
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Liczba unikalnych URL, które crawler widział (kolejka + odwiedzone)
     */
    public int visitedSize() {
        return seen.size();
    }

    /**
     * Liczba wszystkich dodanych (łącznie z odrzuconymi)
     */
    public long totalAdded() {
        return totalAdded;
    }

    /**
     * Reset (jeśli chcesz od nowa)
     */
    public void reset() {
        queue.clear();
        seen.clear();
        totalAdded = 0;
    }

    /**
     * Normalizuje adres URL, żeby unikać duplikatów typu "/" vs bez "/"
     */
    private String normalize(String url) {
        try {
            java.net.URL u = new java.net.URL(url);
            String path = u.getPath();
            if (!path.endsWith("/") && !path.contains(".")) {
                path += "/";  // dodaj / tylko jeśli to katalog, nie plik
            }
            return (u.getProtocol() + "://" + u.getHost() +
                    (u.getPort() > 0 ? ":" + u.getPort() : "") +
                    path).toLowerCase();
        } catch (Exception e) {
            return url.toLowerCase();
        }
    }

}

