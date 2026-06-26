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

        // endpoint do wyświetlania stron w HTML z odświeżaniem co 5 sekund
        Spark.get("/", (req, res) -> {
            StringBuilder html = new StringBuilder("<html><head><title>MagicBlackSpider</title>");
            html.append("<meta http-equiv='refresh' content='5'>"); // odśwież co 5 sekund
            html.append("<style>");
            html.append(".depth0 { color: green; }");
            html.append(".depth1 { color: blue; }");
            html.append(".depth2 { color: orange; }");
            html.append(".depth3 { color: red; }");
            html.append("body { background-color: gray; margin: 0; padding: 16px; }");
            html.append("#stats {");
            html.append("  position: fixed; top: 16px; right: 16px;");
            html.append("  background: #1a1a2e; color: #eee;");
            html.append("  border: 1px solid #444; border-radius: 8px;");
            html.append("  padding: 14px 18px; min-width: 200px;");
            html.append("  font-family: monospace; font-size: 13px;");
            html.append("  box-shadow: 0 4px 12px rgba(0,0,0,0.5);");
            html.append("  z-index: 999;");
            html.append("}");
            html.append("#stats h3 { margin: 0 0 10px 0; font-size: 14px; color: #aaa; letter-spacing: 1px; text-transform: uppercase; }");
            html.append("#stats .total { font-size: 22px; font-weight: bold; color: #fff; margin-bottom: 10px; }");
            html.append("#stats .row { display: flex; justify-content: space-between; margin: 4px 0; }");
            html.append("#stats .dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }");
            html.append("#stats .c0 { background: green; } #stats .c1 { background: #4488ff; }");
            html.append("#stats .c2 { background: orange; } #stats .c3 { background: red; }");
            html.append("#stats .cnt { font-weight: bold; }");
            html.append("</style>");
            html.append("</head><body>");

            List<String> snapshot = new ArrayList<>(messageQueue);

            // count per depth for the stats panel
            Map<Integer, Integer> depthCounts = new TreeMap<>();
            int counter = 1;
            for (String msg : snapshot) {
                String[] parts = msg.split("\\|", 2);
                int depth = 0;
                if (parts.length == 2) {
                    try { depth = Integer.parseInt(parts[0]); } catch (NumberFormatException ignored) {}
                }
                depthCounts.merge(depth, 1, Integer::sum);
            }

            // stats panel
            html.append("<div id='stats'>");
            html.append("<h3>Crawler Stats</h3>");
            html.append("<div class='total'>").append(snapshot.size()).append(" pages</div>");
            String[] depthLabels = {"Depth 0", "Depth 1", "Depth 2", "Depth 3+"};
            for (Map.Entry<Integer, Integer> e : depthCounts.entrySet()) {
                int d = e.getKey();
                int colorIdx = Math.min(d, 3);
                String label = d <= 3 ? depthLabels[d] : "Depth " + d;
                html.append("<div class='row'>")
                    .append("<span><span class='dot c").append(colorIdx).append("'></span>").append(label).append("</span>")
                    .append("<span class='cnt'>").append(e.getValue()).append("</span>")
                    .append("</div>");
            }
            html.append("</div>");

            html.append("<h1>Przetworzone strony</h1><ul>");

            counter = 1;
            for (String msg : snapshot) {
                // spodziewany format: depth|url
                String[] parts = msg.split("\\|", 2);
                int depth = 0;
                String url = msg;
                if (parts.length == 2) {
                    try {
                        depth = Integer.parseInt(parts[0]);
                        url = parts[1];
                    } catch (NumberFormatException e) {
                        // zostaw domyślną głębokość 0
                    }
                }

                String cssClass = "depth" + (depth > 3 ? 3 : depth);
                html.append("<li class='").append(cssClass).append("'>")
                        .append(String.format("%03d: ", counter))
                        .append(url)
                        .append("</li>");
                counter++;
            }

            html.append("</ul></body></html>");
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
}
