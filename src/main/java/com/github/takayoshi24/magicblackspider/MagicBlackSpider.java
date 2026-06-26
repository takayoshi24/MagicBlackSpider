package com.github.takayoshi24.magicblackspider;

import com.github.takayoshi24.magicblackspider.fetcher.Fetcher;
import com.github.takayoshi24.magicblackspider.handler.PageHandler;
import com.github.takayoshi24.magicblackspider.utils.PolitenessManager;
import com.github.takayoshi24.magicblackspider.utils.RobotsTxtChecker;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MagicBlackSpider {

    private static final Logger logger = LoggerFactory.getLogger(MagicBlackSpider.class);

    private final Scheduler scheduler;
    private final Fetcher fetcher;
    private final PageHandler handler;
    private final ExecutorService executor;
    private final long politenessMillis;

    private final PolitenessManager politenessManager;
    private final RobotsTxtChecker robotsChecker;
    private final AtomicInteger processed = new AtomicInteger(0);

    public MagicBlackSpider(Scheduler scheduler, Fetcher fetcher, PageHandler handler, int threads, long politenessMillis) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.handler = handler;
        this.executor = Executors.newFixedThreadPool(threads);
        this.politenessMillis = politenessMillis;
        this.politenessManager = new PolitenessManager(politenessMillis);
        this.robotsChecker = new RobotsTxtChecker();
    }

    public void start(String seedUrl, int maxPages) throws InterruptedException {
        // Dodaj URL startowy z depth = 0
        scheduler.add(seedUrl, 0);

        // Główna pętla: pobieraj URL-e i submituj do executor
        while (processed.get() < maxPages) {
            Scheduler.UrlWithDepth urlWithDepth = scheduler.next();

            if (urlWithDepth == null) {
                // Kolejka pusta – poczekaj chwilę i spróbuj ponownie
                if (scheduler.isEmpty()) {
                    Thread.sleep(100);
                }
                continue;
            }

            // Jeśli poison pill → zakończ pętlę
            if (urlWithDepth == Scheduler.POISON_PILL) {
                logger.info("Otrzymano poison pill → kończymy crawl");
                break;
            }

            // Submit tasku do executor
            executor.submit(() -> {
                try {
                    String url = urlWithDepth.url;
                    int depth = urlWithDepth.depth;

                    // Sprawdzenie robots.txt
                    if (!robotsChecker.isAllowed(url)) return;

                    // Politeness
                    politenessManager.ensurePolite(url);

                    // Pobierz stronę
                    Document doc = fetcher.fetch(url);
                    if (doc == null) return;

                    // Obsłuż stronę z depth
                    handler.handle(new Page(url, doc), scheduler, depth);

                    // Oznacz jako odwiedzoną
                    scheduler.markVisited(url);

                    // Zwiększ licznik przetworzonych
                    processed.incrementAndGet();

                } catch (Exception e) {
                    logger.warn("Błąd przy przetwarzaniu URL {}: {}", urlWithDepth.url, e.getMessage());
                }
            });
        }

        // Wszystkie URL-e submitowane → dodaj poison pill
        scheduler.addPoisonPill();
        logger.info("Dodano PoisonPill do kolejki");

        // Zatrzymaj executor i poczekaj aż wszystkie wątki zakończą się
        executor.shutdown();
        logger.info("Executor zatrzymany, oczekiwanie na zakończenie wątków...");

        if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
            executor.shutdownNow();
            logger.warn("Executor nie zakończył pracy w czasie, wymuszone shutdownNow()");
        }

        logger.info("=== Koniec crawl’a ===");
    }


    public int getProcessedCount() {
        return processed.get();
    }
}
