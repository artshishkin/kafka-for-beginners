package com.artarkatesoft.learnkafka.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumerCommitSpecificOffset {

    Logger log = LoggerFactory.getLogger(MessageConsumerCommitSpecificOffset.class);

    KafkaConsumer<String, String> kafkaConsumer;
    private final String topicName = "test-topic-replicated";

    private Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

    public MessageConsumerCommitSpecificOffset(Map<String, Object> properties) {
        this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
    }

    public static Map<String, Object> buildConsumerProperties() {

        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "message_consumer2");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        return properties;
    }

    public void pollKafka() {

        kafkaConsumer.subscribe(List.of(topicName));

        try {
            log.info("Polling topic: {}", topicName);
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                consumerRecords.forEach(record -> {
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    offsetMap.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));

                    log.info("Record key: {}, value: {}, partition: {}, offset: {}",
                            record.key(), record.value(), record.partition(), record.offset());
                });

                if (!consumerRecords.isEmpty()) {
                    log.info("Commit sync of count {}", consumerRecords.count());
                    log.info("Offset map is: {}", offsetMap);
                    kafkaConsumer.commitSync(offsetMap); //the last record offset returned by the poll
                    offsetMap.clear();
                }
            }
        } catch (CommitFailedException e) {
            log.error("CommitFailedException while polling broker: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Exception while polling broker: {}", e.getMessage());
        } finally {
            kafkaConsumer.close();
            log.info("Closing Kafka consumer");
        }
    }

    public static void main(String[] args) {
        MessageConsumerCommitSpecificOffset messageConsumer = new MessageConsumerCommitSpecificOffset(buildConsumerProperties());
        messageConsumer.pollKafka();
    }
}
