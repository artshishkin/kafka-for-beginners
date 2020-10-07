package com.artarkatesoft.learnkafka.producers;

import org.apache.kafka.clients.producer.*;
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

    public static Map<String, Object> propsMap() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    public void publishMessageSync(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
        Future<RecordMetadata> future = kafkaProducer.send(record);
        try {
            RecordMetadata recordMetadata = future.get();
            log.info("Message `{}` sent successfully for the key `{}`", value, key);
            log.info("Published Message Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception in publishMessageSync : {}", e.getMessage());
        }
    }

    public Future<RecordMetadata> publishMessageAsync(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
        return kafkaProducer.send(record, asyncPublishCallback);
    }

    private Callback asyncPublishCallback = (recordMetadata, exception) -> {
        if (exception == null)
            log.info("Published Message Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
        else
            log.error("Exception in publishMessageAsync : {}", exception.getMessage());
    };

    public static void main(String[] args) throws InterruptedException {
        MessageProducer messageProducer = new MessageProducer(propsMap());
        messageProducer.publishMessageSync("1", "These messages");
        messageProducer.publishMessageSync("1", "must be in the same partitions");
        messageProducer.publishMessageSync("1", "And order is guarantee to be the same");

//        Future<RecordMetadata> asynchronousMessageFromCode = messageProducer.publishMessageAsync(null, "Asynchronous message from code");
//        while (!asynchronousMessageFromCode.isDone());
//        Thread.sleep(100);
    }

    public void close() {
        kafkaProducer.close();
    }
}
