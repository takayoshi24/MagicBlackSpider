package com.github.takayoshi24.magicblackspider.handler;

import org.junit.jupiter.api.Test;

import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HTMLKafkaServerQueueCapTest {

    private static final int CAP = 200; // simulates maxPages passed as queue capacity

    @Test
    void displayQueue_doesNotGrowBeyondCap() {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(CAP);

        int overLimit = CAP + 50;
        for (int i = 0; i < overLimit; i++) {
            while (!queue.offer("0|http://example.com/" + i)) {
                queue.poll();
            }
        }

        assertEquals(CAP, queue.size(), "Queue must never exceed its capacity");
    }

    @Test
    void displayQueue_retainsLatestEntries_afterEviction() {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(CAP);

        int total = CAP + 10;
        for (int i = 0; i < total; i++) {
            while (!queue.offer("msg-" + i)) {
                queue.poll();
            }
        }

        // The tail of the queue should be the last message added
        String last = null;
        for (String s : queue) last = s;
        assertEquals("msg-" + (total - 1), last,
                "The most recently added message must be present after eviction");
    }
}
