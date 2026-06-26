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
import java.util.concurrent.Semaphore;
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
        this(scheduler, fetcher, handler, threads, politenessMillis,
                new PolitenessManager(politenessMillis), new RobotsTxtChecker());
    }

    MagicBlackSpider(Scheduler scheduler, Fetcher fetcher, PageHandler handler, int threads,
                     long politenessMillis, PolitenessManager politenessManager, RobotsTxtChecker robotsChecker) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.handler = handler;
        this.executor = Executors.newFixedThreadPool(threads);
        this.politenessMillis = politenessMillis;
        this.politenessManager = politenessManager;
        this.robotsChecker = robotsChecker;
    }

    public void start(String seedUrl, int maxPages) throws InterruptedException {
        // Dodaj URL startowy z depth = 0
        scheduler.add(seedUrl, 0);

        // Semaphore limits total submitted tasks to maxPages, preventing overshoot
        // under concurrent processing (issue #11).
        Semaphore pagePermits = new Semaphore(maxPages);

        // Główna pętla: pobieraj URL-e i submituj do executor
        while (pagePermits.tryAcquire(500, TimeUnit.MILLISECONDS)) {
            Scheduler.UrlWithDepth urlWithDepth = scheduler.next(500, TimeUnit.MILLISECONDS);

            if (urlWithDepth == null) {
                pagePermits.release(); // no URL available yet, give permit back
                continue;
            }

            // Jeśli poison pill → zakończ pętlę
            if (urlWithDepth == Scheduler.POISON_PILL) {
                logger.info("Otrzymano poison pill → kończymy crawl");
                pagePermits.release();
                break;
            }

            final String url = urlWithDepth.url;
            final int depth = urlWithDepth.depth;

            // Robots.txt + politeness checks at dispatch time so worker threads never sleep.
            try {
                String baseUrl = new java.net.URL(url).getProtocol() + "://" + new java.net.URL(url).getHost();
                RobotsTxtChecker.RobotsTxtRules rules = robotsChecker.fetchRules(baseUrl);

                if (!robotsChecker.isAllowed(url, rules, baseUrl)) {
                    // Disallowed URLs consume a permit to bound maxPages, but are not submitted.
                    continue;
                }

                // If the host isn't ready yet, release the permit, re-queue the URL, and wait
                // briefly before the next dispatch iteration.
                long waitMs = politenessManager.tryAcquire(url, rules.crawlDelayMillis);
                if (waitMs > 0) {
                    pagePermits.release();
                    scheduler.requeue(urlWithDepth);
                    TimeUnit.MILLISECONDS.sleep(Math.min(waitMs, 100));
                    continue;
                }
            } catch (Exception e) {
                logger.warn("Błąd przy dispatch URL {}: {}", url, e.getMessage());
                continue;
            }

            // Submit tasku do executor; permit is intentionally NOT released —
            // each acquired permit represents one page slot consumed.
            executor.submit(() -> {
                try {
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
                    logger.warn("Błąd przy przetwarzaniu URL {}: {}", url, e.getMessage());
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
