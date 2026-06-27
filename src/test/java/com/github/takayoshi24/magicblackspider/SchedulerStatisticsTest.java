package com.github.takayoshi24.magicblackspider;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    @Test
    void visitedSize_isZeroAfterResetUnderConcurrentMarkVisited() throws InterruptedException {
        // Regression test for race: markVisited() must hold lock so it cannot
        // interleave with reset(), preventing ghost increments surviving a reset.
        Scheduler scheduler = new Scheduler();
        int workers = 20;
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(workers);

        for (int i = 0; i < workers; i++) {
            int n = i;
            Thread t = new Thread(() -> {
                try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                for (int j = 0; j < 500; j++) {
                    scheduler.markVisited("http://example.com/" + n + "/" + j);
                }
                done.countDown();
            });
            t.setDaemon(true);
            t.start();
        }

        go.countDown();
        Thread.sleep(1); // let some markVisited() calls accumulate
        scheduler.reset();
        done.await(5, TimeUnit.SECONDS);

        scheduler.reset();
        assertEquals(0, scheduler.visitedSize(),
                "visitedSize() must be 0 after reset regardless of concurrent markVisited() calls");
    }
}
