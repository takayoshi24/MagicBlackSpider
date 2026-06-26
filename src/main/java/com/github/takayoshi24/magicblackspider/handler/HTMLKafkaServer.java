package com.github.takayoshi24.magicblackspider.handler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import spark.Spark;

import java.time.Duration;
import java.util.ArrayList;
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

    private final KafkaConsumer<String, String> consumer;
    private final BlockingQueue<String> messageQueue;
    private volatile boolean running = false;

    public HTMLKafkaServer(String bootstrapServers, String topic, BlockingQueue<String> messageQueue) {
        this.messageQueue = messageQueue;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "magicblackspider-html-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void startServer(int port) {
        Spark.port(port);

        Spark.get("/", (req, res) -> {
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
            html.append("#stats{position:fixed;top:24px;right:24px;background:#13131f;border:1px solid #2a2a3e;border-radius:10px;padding:16px 20px;min-width:230px;z-index:999;box-shadow:0 4px 20px rgba(0,0,0,0.6)}");
            html.append("#stats h3{font-size:11px;color:#666;letter-spacing:2px;text-transform:uppercase;margin-bottom:12px}");
            html.append("#stats .total{font-size:26px;font-weight:bold;color:#fff;margin-bottom:14px}");
            html.append("#stats .total span{font-size:13px;color:#555;font-weight:normal;margin-left:4px}");
            html.append("#stats .row{display:flex;justify-content:space-between;align-items:center;margin:5px 0;font-size:12px}");
            html.append("#stats .dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px;flex-shrink:0}");
            html.append("#stats .cnt{font-weight:bold;color:#fff}");
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
            html.append("<div id='stats'>");
            html.append("<h3>Crawler Stats</h3>");
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
                html.append("<td><a class='url-link' href='").append(escapeHtml(url)).append("' target='_blank'>");
                html.append("<div class='host-row'>");
                if (!scheme.isEmpty()) html.append("<span class='scheme'>").append(escapeHtml(scheme)).append("</span>");
                html.append("<span class='host'>").append(escapeHtml(host)).append("</span>");
                html.append("</div>");
                if (!path.equals("/")) html.append("<span class='path'>").append(escapeHtml(path)).append("</span>");
                html.append("</a></td>");
                html.append("</tr>");
            }

            html.append("</tbody></table></body></html>");
            return html.toString();
        });

        // w tle pobieranie z Kafki i dodawanie do kolejki
        running = true;
        Thread consumerThread = new Thread(() -> {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    messageQueue.offer(record.value());
                }
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    public void stopServer() {
        running = false;
        consumer.close();
        Spark.stop();
    }

    // HSL golden-angle distribution — each depth gets a visually distinct hue
    private static String depthColor(int depth) {
        double hue = (depth * 137.508) % 360;
        return String.format("hsl(%.0f,65%%,58%%)", hue);
    }

    private static String escapeHtml(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#x27;");
    }
}
