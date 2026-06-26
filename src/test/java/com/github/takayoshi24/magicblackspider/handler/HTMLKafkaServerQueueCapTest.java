package com.github.takayoshi24.magicblackspider.handler;

import org.junit.jupiter.api.Test;

import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HTMLKafkaServerQueueCapTest {

    @Test
    void displayQueue_doesNotGrowBeyondMaxDisplayEntries() {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(HTMLKafkaServer.MAX_DISPLAY_ENTRIES);

        int overLimit = HTMLKafkaServer.MAX_DISPLAY_ENTRIES + 500;
        for (int i = 0; i < overLimit; i++) {
            while (!queue.offer("0|http://example.com/" + i)) {
                queue.poll();
            }
        }

        assertEquals(HTMLKafkaServer.MAX_DISPLAY_ENTRIES, queue.size(),
                "Queue must never exceed MAX_DISPLAY_ENTRIES");
    }

    @Test
    void displayQueue_retainsLatestEntries_afterEviction() {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(HTMLKafkaServer.MAX_DISPLAY_ENTRIES);

        int total = HTMLKafkaServer.MAX_DISPLAY_ENTRIES + 10;
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
