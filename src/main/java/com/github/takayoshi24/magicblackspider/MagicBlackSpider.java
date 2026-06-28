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
    private final int threads;
    private final long politenessMillis;

    private final PolitenessManager politenessManager;
    private final RobotsTxtChecker robotsChecker;
    private final AtomicInteger processed = new AtomicInteger(0);
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicInteger failedCount = new AtomicInteger(0);
    private final AtomicInteger robotsBlockedCount = new AtomicInteger(0);
    private final java.util.concurrent.atomic.AtomicBoolean paused = new java.util.concurrent.atomic.AtomicBoolean(false);

    public MagicBlackSpider(Scheduler scheduler, Fetcher fetcher, PageHandler handler, int threads, long politenessMillis) {
        this(scheduler, fetcher, handler, threads, politenessMillis,
                new PolitenessManager(politenessMillis), new RobotsTxtChecker());
    }

    MagicBlackSpider(Scheduler scheduler, Fetcher fetcher, PageHandler handler, int threads,
                     long politenessMillis, PolitenessManager politenessManager, RobotsTxtChecker robotsChecker) {
        this.scheduler = scheduler;
        this.fetcher = fetcher;
        this.handler = handler;
        this.threads = threads;
        this.politenessMillis = politenessMillis;
        this.politenessManager = politenessManager;
        this.robotsChecker = robotsChecker;
    }

    public void start(String seedUrl, int maxPages) throws InterruptedException {
        processed.set(0);
        inFlight.set(0);
        failedCount.set(0);
        robotsBlockedCount.set(0);
        paused.set(false);
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        // Add seed only when explicitly provided (null means wait for URL from the UI)
        if (seedUrl != null && !seedUrl.isBlank()) {
            scheduler.add(seedUrl, 0);
        }

        // Semaphore limits total submitted tasks to maxPages, preventing overshoot
        // under concurrent processing (issue #11).
        Semaphore pagePermits = new Semaphore(maxPages);

        // False until the first URL is dequeued; prevents early exit while waiting
        // for the user to submit a domain via the web UI.
        boolean everReceivedUrl = seedUrl != null && !seedUrl.isBlank();

        // Główna pętla: pobieraj URL-e i submituj do executor
        while (pagePermits.tryAcquire(500, TimeUnit.MILLISECONDS)) {
            if (paused.get()) {
                pagePermits.release();
                TimeUnit.MILLISECONDS.sleep(100);
                continue;
            }

            Scheduler.UrlWithDepth urlWithDepth = scheduler.next(500, TimeUnit.MILLISECONDS);

            if (urlWithDepth == null) {
                pagePermits.release(); // no URL available yet, give permit back
                if (everReceivedUrl && inFlight.get() == 0) {
                    break; // queue empty and no worker can add more URLs — site exhausted
                }
                continue;
            }

            everReceivedUrl = true;

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
                java.net.URI parsed = java.net.URI.create(url);
                String baseUrl = parsed.getScheme() + "://" + parsed.getAuthority();
                RobotsTxtChecker.RobotsTxtRules rules = robotsChecker.fetchRules(baseUrl);

                if (!robotsChecker.isAllowed(url, rules, baseUrl)) {
                    logger.info("[ROBOTS] Denied by robots.txt: {}", url);
                    robotsBlockedCount.incrementAndGet();
                    pagePermits.release();
                    continue;
                }

                // If the host isn't ready yet, release the permit, re-queue the URL, and wait
                // briefly before the next dispatch iteration.
                long waitMs = politenessManager.tryAcquire(url, rules.crawlDelayMillis);
                if (waitMs > 0) {
                    logger.debug("[POLITENESS] Requeued {} — host not ready for {}ms", url, waitMs);
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
            inFlight.incrementAndGet();
            executor.submit(() -> {
                try {
                    Document doc;
                    try {
                        doc = fetcher.fetch(url);
                    } catch (java.io.IOException e) {
                        failedCount.incrementAndGet();
                        logger.warn("Błąd pobierania URL {}: {}", url, e.getMessage());
                        return;
                    }
                    if (doc == null) { failedCount.incrementAndGet(); return; }

                    handler.handle(new Page(url, doc), scheduler, depth);
                    scheduler.markVisited(url);
                    processed.incrementAndGet();

                } catch (Exception e) {
                    logger.warn("Błąd przy przetwarzaniu URL {}: {}", url, e.getMessage());
                } finally {
                    inFlight.decrementAndGet();
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


    public int getProcessedCount() { return processed.get(); }
    public int getInFlight() { return inFlight.get(); }
    public int getFailedCount() { return failedCount.get(); }
    public int getRobotsBlockedCount() { return robotsBlockedCount.get(); }
    public void pause() { paused.set(true); }
    public void resume() { paused.set(false); }
    public boolean isPaused() { return paused.get(); }
}
