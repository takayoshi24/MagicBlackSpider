package com.github.takayoshi24.magicblackspider;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Scheduler {

    static final int MAX_SEEN_URLS = 500_000;

    public static final UrlWithDepth POISON_PILL = new UrlWithDepth("POISON_PILL", -1);

    private final LinkedBlockingQueue<UrlWithDepth> queue = new LinkedBlockingQueue<>();
    private final Set<String> allUrls = Collections.newSetFromMap(new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > MAX_SEEN_URLS;
        }
    });
    private final ReentrantLock lock = new ReentrantLock();
    private int rejectedCount = 0;
    private final AtomicInteger visitedCount = new AtomicInteger(0);
    private volatile boolean poisonPillAdded = false;

    public static class UrlWithDepth {
        public final String url;
        public final int depth;
        public UrlWithDepth(String url, int depth) {
            this.url = url;
            this.depth = depth;
        }
    }

    // dodanie URL – atomowo sprawdza i dodaje
    public boolean add(String url, int depth) {
        lock.lock();
        try {
            if (allUrls.contains(url)) {
                rejectedCount++;
                return false;
            }
            allUrls.add(url);
            queue.offer(new UrlWithDepth(url, depth));
            return true;
        } finally {
            lock.unlock();
        }
    }

    public UrlWithDepth next(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public void markVisited(String url) {
        visitedCount.incrementAndGet();
    }

    public void addPoisonPill() {
        poisonPillAdded = true;
        queue.offer(POISON_PILL);
    }

    /** Re-adds a URL that was already deduped; bypasses the allUrls check. */
    public void requeue(UrlWithDepth urlWithDepth) {
        queue.offer(urlWithDepth);
    }

    public int queueSize() {
        int size = queue.size();
        return poisonPillAdded ? Math.max(0, size - 1) : size;
    }

    public int visitedSize() {
        return visitedCount.get();
    }

    public int getRejectedCount() {
        lock.lock();
        try {
            return rejectedCount;
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        lock.lock();
        try {
            queue.clear();
            allUrls.clear();
            rejectedCount = 0;
            visitedCount.set(0);
            poisonPillAdded = false;
        } finally {
            lock.unlock();
        }
    }

}
