package com.github.takayoshi24.magicblackspider.handler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import spark.Spark;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * HTML server do podglądu przetworzonych stron z Kafka z numeracją i kolorowaniem wg głębokości.
 */
public class HTMLKafkaServer {

    private final KafkaConsumer<String, String> consumer;
    private final BlockingQueue<String> messageQueue;

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
            html.append("body { background-color: gray; }");
            html.append("</style>");
            html.append("</head><body>");
            html.append("<h1>Przetworzone strony</h1><ul>");

            // snapshot kolejki, aby uniknąć ConcurrentModificationException
            BlockingQueue<String> snapshot = messageQueue; // kopiujemy referencję do bieżącej kolejki

            int counter = 1;
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

                String cssClass = "depth" + (depth > 3 ? 3 : depth); // maksymalna klasa depth3
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
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    messageQueue.offer(record.value());
                }
            }
        }).start();
    }

    // Opcjonalnie można dodać metodę stopServer() do zamykania Spark i konsumenta
    public void stopServer() {
        consumer.close();
        Spark.stop();
    }
}
