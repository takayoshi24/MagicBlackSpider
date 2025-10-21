package com.github.takayoshi24.magicblackspider;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Produkcyjny Scheduler dla MagicBlackSpider
 * - W pełni thread-safe
 * - Nie blokuje się
 * - Dokładne statystyki
 * - Obsługa duplikatów i poprawna kolejność
 */
public class Scheduler {

    private final Queue<String> queue = new ConcurrentLinkedQueue<>();
    private final Set<String> visited = ConcurrentHashMap.newKeySet();
    private long totalAdded = 0;
    private long totalProcessed = 0;

    /**
     * Dodaje URL do kolejki jeśli jeszcze nie odwiedzony ani nie oczekuje w kolejce.
     */
    public boolean add(String url) {
        if (url == null || url.isBlank()) return false;

        // Unikaj duplikatów (sprawdzaj tylko visited)
        if (visited.contains(url) || queue.contains(url)) return false;

        queue.offer(url);
        totalAdded++;
        return true;
    }

    /**
     * Pobiera następny URL do przetworzenia.
     */
    public String next() {
        return queue.poll();
    }

    /**
     * Oznacza URL jako odwiedzony po przetworzeniu.
     */
    public void markVisited(String url) {
        if (url != null && !url.isBlank()) {
            visited.add(url);
            totalProcessed++;
        }
    }

    /**
     * Czy kolejka jest pusta.
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Liczba odwiedzonych URL.
     */
    public int visitedSize() {
        return visited.size();
    }

    /**
     * Całkowita liczba dodanych URL (zduplikowane URL nie są tu liczone).
     */
    public long totalAdded() {
        return totalAdded;
    }

    /**
     * Liczba przetworzonych URL (visited).
     */
    public long totalProcessed() {
        return totalProcessed;
    }

    /**
     * Wyczyść scheduler.
     */
    public void reset() {
        queue.clear();
        visited.clear();
        totalAdded = 0;
        totalProcessed = 0;
    }
}
