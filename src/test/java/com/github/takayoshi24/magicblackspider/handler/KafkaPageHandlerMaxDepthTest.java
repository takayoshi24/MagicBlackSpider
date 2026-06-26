package com.github.takayoshi24.magicblackspider.handler;

import com.github.takayoshi24.magicblackspider.Page;
import com.github.takayoshi24.magicblackspider.Scheduler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;

import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaPageHandlerMaxDepthTest {

    private static final String BASE = "http://example.com";
    private static final String HTML_WITH_LINKS =
            "<html><body>" +
            "<a href=\"http://example.com/a\">A</a>" +
            "<a href=\"http://example.com/b\">B</a>" +
            "</body></html>";

    private Page pageAt(String url) {
        Document doc = Jsoup.parse(HTML_WITH_LINKS, url);
        return new Page(url, doc);
    }

    @Test
    void handle_addsLinks_whenDepthBelowMax() {
        Scheduler scheduler = new Scheduler();
        KafkaPageHandler handler = new KafkaPageHandler(new LinkedBlockingQueue<>(), 2);

        handler.handle(pageAt(BASE + "/start"), scheduler, 1);

        assertEquals(2, scheduler.queueSize(), "Links at depth 2 should be queued when maxDepth=2");
    }

    @Test
    void handle_skipsLinks_whenDepthAtMax() {
        Scheduler scheduler = new Scheduler();
        KafkaPageHandler handler = new KafkaPageHandler(new LinkedBlockingQueue<>(), 2);

        handler.handle(pageAt(BASE + "/start"), scheduler, 2);

        assertEquals(0, scheduler.queueSize(), "No links should be queued when current depth equals maxDepth");
    }

    @Test
    void handle_defaultConstructor_imposesNoDepthLimit() {
        Scheduler scheduler = new Scheduler();
        KafkaPageHandler handler = new KafkaPageHandler(new LinkedBlockingQueue<>());

        handler.handle(pageAt(BASE + "/start"), scheduler, 10_000);

        assertEquals(2, scheduler.queueSize(), "Default handler should queue links regardless of depth");
    }
}
