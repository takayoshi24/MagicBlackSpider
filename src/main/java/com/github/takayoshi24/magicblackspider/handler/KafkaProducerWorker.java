package com.github.takayoshi24.magicblackspider.handler;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class KafkaProducerWorker {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerWorker.class);
    private static final String POISON_PILL = "__KAFKA_PRODUCER_STOP__";

    private final KafkaProducer<String, String> producer;
    private final BlockingQueue<String> queue;
    private final String topic;
    private Thread workerThread;

    public KafkaProducerWorker(String bootstrapServers, String topic, BlockingQueue<String> queue) {
        this.topic = topic;
        this.queue = queue;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                System.getenv().getOrDefault("KAFKA_SSL_TRUSTSTORE_LOCATION", "certs/kafka.truststore.jks"));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                System.getenv().getOrDefault("KAFKA_SSL_STORE_PASSWORD", "changeit"));
        // Disable hostname verification for self-signed dev certs; the CA trust anchor is still enforced.
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        this.producer = new KafkaProducer<>(props);
    }

    public void start() {
        workerThread = new Thread(() -> {
            try {
                while (true) {
                    String url = queue.take();
                    if (POISON_PILL.equals(url)) break;
                    producer.send(new ProducerRecord<>(topic, url, url),
                            (metadata, ex) -> {
                                if (ex != null) {
                                    logger.warn("Failed to send record to Kafka: {}", ex.getMessage());
                                }
                            });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("KafkaProducerWorker interrupted");
            } finally {
                producer.flush();
                producer.close();
                logger.info("KafkaProducerWorker stopped");
            }
        });
        workerThread.setName("kafka-producer-worker");
        workerThread.start();
        logger.info("KafkaProducerWorker started, publishing to topic '{}'", topic);
    }

    public void stop() {
        queue.offer(POISON_PILL);
    }

    public void awaitStop() throws InterruptedException {
        if (workerThread != null) {
            workerThread.join();
        }
    }
}
