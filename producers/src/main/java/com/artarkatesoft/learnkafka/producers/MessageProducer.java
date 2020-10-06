package com.artarkatesoft.learnkafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MessageProducer {

    Logger log = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic-replicated";

    private final KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> propsMap) {
        kafkaProducer = new KafkaProducer<>(propsMap);
    }

    static Map<String, Object> propsMap() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public void publishMessageSync(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
        Future<RecordMetadata> future = kafkaProducer.send(record);
        try {
            RecordMetadata recordMetadata = future.get();
            log.info("Record metadata is {}", recordMetadata);
            log.info("partition: {}, offset: {}", recordMetadata.partition(), recordMetadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer(propsMap());
        messageProducer.publishMessageSync(null, "First message from code");
    }

}
