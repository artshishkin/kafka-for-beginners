package com.artarkatesoft.learnkafka.consumers;

import com.artarkatesoft.learnkafka.listeners.MessageRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RebalanceListenerMessageConsumer {

    Logger log = LoggerFactory.getLogger(RebalanceListenerMessageConsumer.class);

    KafkaConsumer<String, String> kafkaConsumer;
    private final String topicName = "test-topic-replicated";

    public RebalanceListenerMessageConsumer(Map<String, Object> properties) {
        this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
    }

    public static Map<String, Object> buildConsumerProperties() {

        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "message_consumer2");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        return properties;
    }

    public void pollKafka() {

        kafkaConsumer.subscribe(List.of(topicName), new MessageRebalanceListener(kafkaConsumer));

        try {
            log.info("Polling topic: {}", topicName);
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                consumerRecords.forEach(record ->
                        log.info("Record key: {}, value: {}, partition: {}, offset: {}",
                                record.key(), record.value(), record.partition(), record.offset()));

                if (!consumerRecords.isEmpty()) {
                    log.info("Commit sync of count {}", consumerRecords.count());
                    kafkaConsumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("Exception while poling broker: {}", e.getMessage());
        } finally {
            kafkaConsumer.close();
            log.info("Closing Kafka consumer");
        }
    }

    public static void main(String[] args) {
        RebalanceListenerMessageConsumer messageConsumer = new RebalanceListenerMessageConsumer(buildConsumerProperties());
        messageConsumer.pollKafka();
    }
}
