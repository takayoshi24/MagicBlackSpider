package com.github.takayoshi24.magicblackspider;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class Scheduler {

    public static final UrlWithDepth POISON_PILL = new UrlWithDepth("POISON_PILL", -1);

    private final LinkedBlockingQueue<UrlWithDepth> queue = new LinkedBlockingQueue<>();
    private final Set<String> allUrls = new HashSet<>(); // zbiór wszystkich URL: dodanych i odwiedzonych
    private final ReentrantLock lock = new ReentrantLock();
    private int rejectedCount = 0;

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
        // teraz wszystkie URL są już w allUrls, nie trzeba nic robić
    }

    public void addPoisonPill() {
        queue.offer(POISON_PILL);
    }

    public int queueSize() {
        return queue.size();
    }

    public int visitedSize() {
        lock.lock();
        try {
            return allUrls.size() - queue.size();
        } finally {
            lock.unlock();
        }
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
        } finally {
            lock.unlock();
        }
    }

}
