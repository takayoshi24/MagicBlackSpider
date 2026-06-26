package com.github.takayoshi24.magicblackspider;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerLruCapTest {

    @Test
    void allUrls_doesNotGrowBeyondMaxSeenUrls() {
        Scheduler scheduler = new Scheduler();
        int overLimit = Scheduler.MAX_SEEN_URLS + 100;

        for (int i = 0; i < overLimit; i++) {
            scheduler.add("http://example.com/" + i, 0);
        }

        // After eviction the earliest URL should be re-addable (evicted from seen set)
        boolean reaccepted = scheduler.add("http://example.com/0", 0);
        assertTrue(reaccepted, "Evicted URL must be accepted again after LRU cap is reached");
    }
}
