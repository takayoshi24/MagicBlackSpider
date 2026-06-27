package com.github.takayoshi24.magicblackspider.handler;

import com.github.takayoshi24.magicblackspider.Scheduler;
import com.github.takayoshi24.magicblackspider.fetcher.SimpleFetcher;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.poi.xwpf.usermodel.ParagraphAlignment;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.javalin.Javalin;
import io.javalin.http.UnauthorizedResponse;

import java.io.ByteArrayOutputStream;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

/**
 * HTML server do podglądu przetworzonych stron z Kafka z numeracją i kolorowaniem wg głębokości.
 */
public class HTMLKafkaServer {

    private static final Logger log = LoggerFactory.getLogger(HTMLKafkaServer.class);

    private final KafkaConsumer<String, String> consumer;
    private final BlockingQueue<String> messageQueue;
    private final Scheduler scheduler;
    private volatile boolean running = false;
    private Thread consumerThread;
    private Javalin app;
    private volatile long crawlStartTime = 0;
    private volatile long crawlEndTime = 0;
    private volatile String seedUrl = "";
    private final String csrfToken;
    private final String apiKey;

    public HTMLKafkaServer(String bootstrapServers, String topic, BlockingQueue<String> messageQueue, Scheduler scheduler) {
        this.messageQueue = messageQueue;
        this.scheduler = scheduler;
        SecureRandom rng = new SecureRandom();
        byte[] tokenBytes = new byte[32];
        rng.nextBytes(tokenBytes);
        this.csrfToken = Base64.getUrlEncoder().withoutPadding().encodeToString(tokenBytes);
        byte[] keyBytes = new byte[24];
        rng.nextBytes(keyBytes);
        this.apiKey = Base64.getUrlEncoder().withoutPadding().encodeToString(keyBytes);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "magicblackspider-html-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        String truststorePath = System.getenv().getOrDefault("KAFKA_SSL_TRUSTSTORE_LOCATION", "certs/kafka.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                System.getenv().getOrDefault("KAFKA_SSL_STORE_PASSWORD", "changeit"));
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        props.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2,TLSv1.3");
        // Disable hostname verification for self-signed dev certs; the CA trust anchor is still enforced.
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        log.debug("[Kafka consumer] truststore: {} exists={}",
                new java.io.File(truststorePath).getAbsolutePath(),
                new java.io.File(truststorePath).exists());

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void startServer(int port) {
        log.info("[AUTH] MagicBlackSpider UI — credentials: admin / {}", apiKey);
        log.info("[AUTH] Open http://127.0.0.1:{}/ in your browser and enter these when prompted.", port);

        app = Javalin.create().start("127.0.0.1", port);

        app.before(ctx -> {
            String auth = ctx.header("Authorization");
            if (auth != null && auth.startsWith("Basic ")) {
                String decoded = new String(java.util.Base64.getDecoder().decode(auth.substring(6)));
                if (("admin:" + apiKey).equals(decoded)) return;
            }
            ctx.header("WWW-Authenticate", "Basic realm=\"MagicBlackSpider\"");
            throw new UnauthorizedResponse("Unauthorized");
        });

        app.after(ctx -> {
            ctx.header("Content-Security-Policy",
                    "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; object-src 'none'");
            ctx.header("X-Frame-Options", "DENY");
            ctx.header("X-Content-Type-Options", "nosniff");
            ctx.header("Referrer-Policy", "no-referrer");
        });

        app.post("/seed", ctx -> {
            if (!csrfToken.equals(ctx.formParam("_csrf"))) {
                ctx.status(403).result("Forbidden");
                return;
            }
            String url = ctx.formParam("url");
            if (url != null && !url.isBlank()) {
                url = url.trim();
                if (!url.startsWith("http://") && !url.startsWith("https://")) {
                    url = "https://" + url;
                }
                if (SimpleFetcher.isBlockedUrl(url)) {
                    ctx.status(400).html("<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Blocked</title>"
                            + "<style>body{background:#0f0f1a;color:#e07070;font-family:'Courier New',monospace;"
                            + "display:flex;align-items:center;justify-content:center;height:100vh;margin:0}"
                            + "div{text-align:center}a{color:#7eb8ff}</style></head><body>"
                            + "<div><h2>Blocked</h2><p>The URL resolves to a private or loopback address "
                            + "and cannot be crawled.</p><p><a href='/'>&#8592; Back</a></p></div></body></html>");
                    return;
                }
                crawlStartTime = System.currentTimeMillis();
                crawlEndTime = 0;
                seedUrl = url;
                scheduler.add(url, 0);
            }
            ctx.redirect("/");
        });

        app.post("/clear", ctx -> {
            if (!csrfToken.equals(ctx.formParam("_csrf"))) {
                ctx.status(403).result("Forbidden");
                return;
            }
            messageQueue.clear();
            crawlStartTime = 0;
            crawlEndTime = 0;
            seedUrl = "";
            ctx.redirect("/");
        });

        app.post("/download", ctx -> {
            if (!csrfToken.equals(ctx.formParam("_csrf"))) {
                ctx.status(403).result("Forbidden");
                return;
            }
            List<String> snapshot = new ArrayList<>(messageQueue);
            Map<Integer, Integer> depthCounts = new TreeMap<>();
            List<int[]> depthList = new ArrayList<>();
            List<String> urlList = new ArrayList<>();
            for (String msg : snapshot) {
                String[] parts = msg.split("\\|", 2);
                int depth = 0;
                String url = msg;
                if (parts.length == 2) {
                    try { depth = Integer.parseInt(parts[0]); url = parts[1]; }
                    catch (NumberFormatException ignored) {}
                }
                depthList.add(new int[]{depth});
                urlList.add(url);
                depthCounts.merge(depth, 1, Integer::sum);
            }

            byte[] docx = generateDocx(urlList, depthList, depthCounts, crawlStartTime, crawlEndTime, seedUrl);

            ctx.contentType("application/vnd.openxmlformats-officedocument.wordprocessingml.document");
            ctx.header("Content-Disposition", "attachment; filename=\"crawl-report.docx\"");
            ctx.result(docx);

            messageQueue.clear();
            crawlStartTime = 0;
            crawlEndTime = 0;
            seedUrl = "";
        });

        app.get("/", ctx -> {
            long startTime = crawlStartTime;
            long endTime = crawlEndTime;
            List<String> snapshot = new ArrayList<>(messageQueue);

            // parse messages and count per depth
            Map<Integer, Integer> depthCounts = new TreeMap<>();
            List<int[]> depthList = new ArrayList<>(); // [depth] per entry
            List<String> urlList = new ArrayList<>();

            for (String msg : snapshot) {
                String[] parts = msg.split("\\|", 2);
                int depth = 0;
                String url = msg;
                if (parts.length == 2) {
                    try { depth = Integer.parseInt(parts[0]); url = parts[1]; }
                    catch (NumberFormatException ignored) {}
                }
                depthList.add(new int[]{depth});
                urlList.add(url);
                depthCounts.merge(depth, 1, Integer::sum);
            }

            StringBuilder html = new StringBuilder("<!DOCTYPE html><html><head>");
            html.append("<meta charset='UTF-8'>");
            html.append("<title>MagicBlackSpider</title>");
            html.append("<meta http-equiv='refresh' content='5'>");
            html.append("<style>");
            html.append("*{box-sizing:border-box;margin:0;padding:0}");
            html.append("body{background:#0f0f1a;color:#ccc;font-family:'Courier New',monospace;padding:24px 280px 24px 24px;min-height:100vh}");
            html.append("h1{color:#fff;font-size:18px;letter-spacing:2px;text-transform:uppercase;margin-bottom:20px;padding-bottom:10px;border-bottom:1px solid #2a2a3e}");
            html.append("#seed-panel{background:#13131f;border:1px solid #2a2a3e;border-radius:10px;padding:16px 20px;margin-bottom:24px;display:flex;align-items:center;gap:12px;flex-wrap:wrap}");
            html.append("#seed-panel label{font-size:11px;color:#666;letter-spacing:2px;text-transform:uppercase;white-space:nowrap}");
            html.append("#seed-panel input[type=text]{flex:1;min-width:260px;background:#0a0a14;border:1px solid #2a2a3e;border-radius:6px;padding:8px 12px;color:#e8e8f0;font-family:'Courier New',monospace;font-size:13px;outline:none}");
            html.append("#seed-panel input[type=text]:focus{border-color:#7eb8ff}");
            html.append("#seed-panel button{background:#1a3a5c;border:1px solid #7eb8ff44;border-radius:6px;padding:8px 18px;color:#7eb8ff;font-family:'Courier New',monospace;font-size:12px;letter-spacing:1px;cursor:pointer;white-space:nowrap}");
            html.append("#seed-panel button:hover{background:#1f4a75;border-color:#7eb8ff}");
            html.append("#stats{position:fixed;top:24px;right:24px;background:#13131f;border:1px solid #2a2a3e;border-radius:10px;padding:16px 20px;min-width:230px;z-index:999;box-shadow:0 4px 20px rgba(0,0,0,0.6)}");
            html.append("#stats h3{font-size:11px;color:#666;letter-spacing:2px;text-transform:uppercase;margin-bottom:12px}");
            html.append("#stats .total{font-size:26px;font-weight:bold;color:#fff;margin-bottom:14px}");
            html.append("#stats .total span{font-size:13px;color:#555;font-weight:normal;margin-left:4px}");
            html.append("#stats .row{display:flex;justify-content:space-between;align-items:center;margin:5px 0;font-size:12px}");
            html.append("#stats .dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px;flex-shrink:0}");
            html.append("#stats .cnt{font-weight:bold;color:#fff}");
            html.append("#stats .timer-label{font-size:10px;color:#555;letter-spacing:1px;text-transform:uppercase;margin-top:14px;margin-bottom:4px}");
            html.append("#stats .timer{font-size:20px;font-weight:bold;color:#7eb8ff;letter-spacing:3px;font-variant-numeric:tabular-nums}");
            html.append(".btn-clear{background:#2a0a0a;border:1px solid #7b2222;border-radius:6px;padding:8px 18px;color:#e07070;font-family:'Courier New',monospace;font-size:12px;letter-spacing:1px;cursor:pointer;white-space:nowrap}");
            html.append(".btn-clear:hover{background:#3a0e0e;border-color:#e07070}");
            html.append(".btn-download{background:#0a2a1a;border:1px solid #226644;border-radius:6px;padding:8px 18px;color:#6fcf97;font-family:'Courier New',monospace;font-size:12px;letter-spacing:1px;cursor:pointer;white-space:nowrap;width:100%;margin-bottom:12px}");
            html.append(".btn-download:hover{background:#0e3a22;border-color:#6fcf97}");
            html.append("table{width:100%;border-collapse:collapse}");
            html.append("thead th{text-align:left;padding:8px 12px;color:#555;font-size:10px;letter-spacing:1px;text-transform:uppercase;border-bottom:1px solid #2a2a3e;font-weight:normal}");
            html.append("tbody tr{border-bottom:1px solid #16161f}");
            html.append("tbody tr:hover{background:rgba(255,255,255,0.02)}");
            html.append("td{padding:9px 12px;vertical-align:middle}");
            html.append(".num{color:#444;font-size:11px;width:50px}");
            html.append(".badge{display:inline-block;padding:2px 9px;border-radius:20px;font-size:10px;font-weight:bold;letter-spacing:0.5px}");
            html.append(".scheme{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;margin-right:6px;background:#1e1e30;color:#667;border:1px solid #2a2a3e}");
            html.append(".host{color:#e8e8f0;font-weight:bold;font-size:13px}");
            html.append(".path{color:#555;font-size:12px;word-break:break-all}");
            html.append("a.url-link{text-decoration:none;display:flex;flex-direction:column;gap:2px}");
            html.append("a.url-link:hover .host{color:#7eb8ff}");
            html.append("a.url-link:hover .path{color:#888}");
            html.append(".host-row{display:flex;align-items:center;flex-wrap:wrap;gap:4px}");
            html.append("</style></head><body>");

            // stats panel
            String csrfField = "<input type='hidden' name='_csrf' value='" + csrfToken + "'/>";
            html.append("<div id='stats'>");
            html.append("<form method='POST' action='/download'>");
            html.append(csrfField);
            html.append("<button type='submit' class='btn-download'>&#x2B07; Download Report &amp; Clear</button>");
            html.append("</form>");
            html.append("<h3>Crawler Stats</h3>");
            html.append("<div class='timer-label'>Crawl Time</div>");
            html.append("<div class='timer' id='crawl-timer'>--:--:--</div>");
            html.append("<div class='total'>").append(snapshot.size()).append("<span>pages crawled</span></div>");
            for (Map.Entry<Integer, Integer> e : depthCounts.entrySet()) {
                int d = e.getKey();
                String color = depthColor(d);
                html.append("<div class='row'>")
                    .append("<span><span class='dot' style='background:").append(color).append("'></span>Depth ").append(d).append("</span>")
                    .append("<span class='cnt'>").append(e.getValue()).append("</span>")
                    .append("</div>");
            }
            html.append("</div>");

            html.append("<div id='seed-panel'>");
            html.append("<label>Add Domain</label>");
            html.append("<form method='POST' action='/seed' style='display:flex;gap:8px;flex:1;flex-wrap:wrap'>");
            html.append(csrfField);
            html.append("<input type='text' name='url' placeholder='https://example.com' />");
            html.append("<button type='submit'>Crawl</button>");
            html.append("</form>");
            html.append("<form method='POST' action='/clear'>");
            html.append(csrfField);
            html.append("<button type='submit' class='btn-clear'>Clear Data</button>");
            html.append("</form>");
            html.append("</div>");
            html.append("<h1>&#x1F577; MagicBlackSpider &mdash; Crawled Pages</h1>");
            html.append("<table>");
            html.append("<thead><tr><th>#</th><th>Depth</th><th>Address</th></tr></thead>");
            html.append("<tbody>");

            for (int i = 0; i < urlList.size(); i++) {
                String url = urlList.get(i);
                int depth = depthList.get(i)[0];
                String color = depthColor(depth);

                String scheme = "";
                String host = url;
                String path = "";
                try {
                    java.net.URI uri = new java.net.URI(url);
                    if (uri.getScheme() != null) scheme = uri.getScheme();
                    if (uri.getHost() != null) host = uri.getHost();
                    String rawPath = uri.getPath() != null ? uri.getPath() : "";
                    String query = uri.getQuery() != null ? "?" + uri.getQuery() : "";
                    path = rawPath + query;
                    if (path.isEmpty()) path = "/";
                } catch (Exception ignored) {}

                html.append("<tr>");
                html.append("<td class='num'>").append(String.format("%03d", i + 1)).append("</td>");
                html.append("<td><span class='badge' style='background:").append(color).append("22;color:").append(color)
                    .append(";border:1px solid ").append(color).append("55'>D").append(depth).append("</span></td>");
                html.append("<td><a class='url-link' href='").append(safeHref(url)).append("' target='_blank'>");
                html.append("<div class='host-row'>");
                if (!scheme.isEmpty()) html.append("<span class='scheme'>").append(escapeHtml(scheme)).append("</span>");
                html.append("<span class='host'>").append(escapeHtml(host)).append("</span>");
                html.append("</div>");
                if (!path.equals("/")) html.append("<span class='path'>").append(escapeHtml(path)).append("</span>");
                html.append("</a></td>");
                html.append("</tr>");
            }

            html.append("</tbody></table>");
            html.append("<script>");
            html.append("var startMs=").append(startTime).append(";");
            html.append("var endMs=").append(endTime).append(";");
            html.append("var el=document.getElementById('crawl-timer');");
            html.append("function pad(n){return String(n).padStart(2,'0');}");
            html.append("function tick(){");
            html.append("  if(startMs===0){el.textContent='--:--:--';return;}");
            html.append("  var ref=endMs!==0?endMs:Date.now();");
            html.append("  var elapsed=Math.floor((ref-startMs)/1000);");
            html.append("  if(elapsed<0)elapsed=0;");
            html.append("  el.textContent=pad(Math.floor(elapsed/3600))+':'+pad(Math.floor((elapsed%3600)/60))+':'+pad(elapsed%60);");
            html.append("}");
            html.append("tick();if(endMs===0)setInterval(tick,1000);");
            html.append("</script>");
            html.append("</body></html>");
            ctx.html(html.toString());
        });

        // w tle pobieranie z Kafki i dodawanie do kolejki
        running = true;
        consumerThread = new Thread(() -> {
            try {
                while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        while (!messageQueue.offer(record.value())) {
                            messageQueue.poll();
                        }
                    }
                }
            } finally {
                // KafkaConsumer is not thread-safe; close must happen on the same thread that calls poll()
                consumer.close();
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    public void signalCrawlFinished() {
        crawlEndTime = System.currentTimeMillis();
    }

    public void stopServer() {
        running = false;
        if (consumerThread != null) {
            try {
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (app != null) {
            app.stop();
        }
    }

    // HSL golden-angle distribution — each depth gets a visually distinct hue
    private static String depthColor(int depth) {
        double hue = (depth * 137.508) % 360;
        return String.format("hsl(%.0f,65%%,58%%)", hue);
    }

    private static String escapeHtml(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#x27;");
    }

    static String safeHref(String url) {
        if (url == null) return "#";
        String lower = url.trim().toLowerCase(java.util.Locale.ROOT);
        return (lower.startsWith("http://") || lower.startsWith("https://")) ? escapeHtml(url) : "#";
    }

    private byte[] generateDocx(List<String> urlList, List<int[]> depthList,
                                  Map<Integer, Integer> depthCounts,
                                  long startTime, long endTime, String seed) throws Exception {
        try (XWPFDocument doc = new XWPFDocument();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            XWPFParagraph title = doc.createParagraph();
            title.setAlignment(ParagraphAlignment.CENTER);
            XWPFRun tr = title.createRun();
            tr.setText("MagicBlackSpider Crawl Report");
            tr.setBold(true);
            tr.setFontSize(18);

            doc.createParagraph().createRun().setText("");

            addInfoLine(doc, "Domain", seed.isEmpty() ? "N/A" : seed);
            addInfoLine(doc, "Exported", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

            String timeStr;
            if (startTime > 0) {
                long ref = endTime > 0 ? endTime : System.currentTimeMillis();
                long secs = (ref - startTime) / 1000;
                timeStr = String.format("%02d:%02d:%02d", secs / 3600, (secs % 3600) / 60, secs % 60);
            } else {
                timeStr = "--:--:--";
            }
            addInfoLine(doc, "Crawl Duration", timeStr);
            addInfoLine(doc, "Total Pages", String.valueOf(urlList.size()));

            doc.createParagraph().createRun().setText("");

            XWPFRun depthHeader = doc.createParagraph().createRun();
            depthHeader.setText("Pages per Depth");
            depthHeader.setBold(true);
            depthHeader.setFontSize(12);

            for (Map.Entry<Integer, Integer> e : depthCounts.entrySet()) {
                addInfoLine(doc, "  Depth " + e.getKey(), e.getValue() + " pages");
            }

            doc.createParagraph().createRun().setText("");

            XWPFRun urlHeader = doc.createParagraph().createRun();
            urlHeader.setText("Crawled URLs");
            urlHeader.setBold(true);
            urlHeader.setFontSize(12);

            if (!urlList.isEmpty()) {
                XWPFTable table = doc.createTable(urlList.size() + 1, 3);
                setCell(table.getRow(0).getCell(0), "#", true);
                setCell(table.getRow(0).getCell(1), "Depth", true);
                setCell(table.getRow(0).getCell(2), "URL", true);
                for (int i = 0; i < urlList.size(); i++) {
                    XWPFTableRow row = table.getRow(i + 1);
                    setCell(row.getCell(0), String.format("%03d", i + 1), false);
                    setCell(row.getCell(1), "D" + depthList.get(i)[0], false);
                    setCell(row.getCell(2), urlList.get(i), false);
                }
            }

            doc.write(out);
            return out.toByteArray();
        }
    }

    private static void addInfoLine(XWPFDocument doc, String label, String value) {
        XWPFParagraph p = doc.createParagraph();
        XWPFRun labelRun = p.createRun();
        labelRun.setText(label + ": ");
        labelRun.setBold(true);
        p.createRun().setText(value);
    }

    private static void setCell(XWPFTableCell cell, String text, boolean bold) {
        XWPFRun r = cell.getParagraphs().get(0).createRun();
        r.setText(text);
        r.setBold(bold);
    }
}
