package com.github.takayoshi24.magicblackspider.handler;

import com.github.takayoshi24.magicblackspider.Page;
import com.github.takayoshi24.magicblackspider.Scheduler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;


public class KafkaPageHandler implements PageHandler {
    private static final Logger logger = LoggerFactory.getLogger(KafkaPageHandler.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;


    public KafkaPageHandler(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }
    int i = 2;
    @Override
    public void handle(Page page, Scheduler scheduler) {
        try {
            String message = page.getDocument().title() + " | " + page.getUrl();
            producer.send(new ProducerRecord<>(topic, page.getUrl(), message));
            logger.info("Wysłano do Kafka: {}", message);


// Możemy też dodawać linki do schedulera
            page.getDocument().select("a[href]").forEach(link -> {
                String next = link.absUrl("href");
                if (next != null && !next.isEmpty()) if(scheduler.add(next)) {
                    logger.info("["+ i +"]"+"Dodano nowy URL do schedulera: {}", next);
                    i++;
                }
            });
        } catch (Exception e) {
            logger.error("Błąd w KafkaPageHandler dla: {}", page.getUrl(), e);
        }
    }


    public void close() {
        producer.close();
    }
}