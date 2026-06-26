package com.github.takayoshi24.magicblackspider;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchedulerStatisticsTest {

    @Test
    void visitedSize_isNotAffectedByRequeuedUrls() {
        Scheduler scheduler = new Scheduler();
        scheduler.add("http://example.com/a", 0);
        Scheduler.UrlWithDepth entry = new Scheduler.UrlWithDepth("http://example.com/a", 0);

        scheduler.markVisited("http://example.com/a");
        // Simulates a requeue: the URL goes back into the queue without touching allUrls
        scheduler.requeue(entry);

        assertEquals(1, scheduler.visitedSize(),
                "visitedSize() must not drop when a URL is requeued");
    }

    @Test
    void visitedSize_isNotAffectedByPoisonPill() {
        Scheduler scheduler = new Scheduler();
        scheduler.add("http://example.com/a", 0);
        scheduler.markVisited("http://example.com/a");

        scheduler.addPoisonPill();

        assertEquals(1, scheduler.visitedSize(),
                "visitedSize() must not change when the poison pill is added");
    }

    @Test
    void queueSize_excludesPoisonPill() throws InterruptedException {
        Scheduler scheduler = new Scheduler();
        scheduler.add("http://example.com/a", 0);
        // Dequeue the URL so only the poison pill remains
        scheduler.next(0, java.util.concurrent.TimeUnit.MILLISECONDS);

        scheduler.addPoisonPill();

        assertEquals(0, scheduler.queueSize(),
                "queueSize() must not count the poison pill");
    }

    @Test
    void visitedSize_tracksMarkVisitedCalls() {
        Scheduler scheduler = new Scheduler();
        scheduler.add("http://example.com/1", 0);
        scheduler.add("http://example.com/2", 0);

        scheduler.markVisited("http://example.com/1");

        assertEquals(1, scheduler.visitedSize());

        scheduler.markVisited("http://example.com/2");

        assertEquals(2, scheduler.visitedSize());
    }
}
