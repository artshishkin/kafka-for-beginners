package com.artarkatesoft.learnkafka.producers;

import com.artarkatesoft.learnkafka.domain.Item;
import com.artarkatesoft.learnkafka.serializers.ItemSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ItemProducer {

    Logger log = LoggerFactory.getLogger(ItemProducer.class);

    String topicName = "items";

    private final KafkaProducer<Integer, Item> kafkaProducer;

    public ItemProducer(Map<String, Object> propsMap) {
        kafkaProducer = new KafkaProducer<>(propsMap);
    }

    public static Map<String, Object> propsMap() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ItemSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 2000);
        return props;
    }

    public void publishMessageSync(Item item) {
        Integer key = item.getId();
        ProducerRecord<Integer, Item> record = new ProducerRecord<>(topicName, key, item);
        Future<RecordMetadata> future = kafkaProducer.send(record);
        try {
            RecordMetadata recordMetadata = future.get();
            log.info("Message `{}` sent successfully for the key `{}`", item, key);
            log.info("Published Message Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception in publishMessageSync : {}", e.getMessage());
        }
    }

    public Future<RecordMetadata> publishMessageAsync(Item item) {
        var record = new ProducerRecord<>(topicName, item.getId(), item);
        return kafkaProducer.send(record, asyncPublishCallback);
    }

    private Callback asyncPublishCallback = (recordMetadata, exception) -> {
        if (exception == null)
            log.info("Published Message Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
        else
            log.error("Exception in publishMessageAsync : {}", exception.getMessage());
    };

    public static void main(String[] args) throws InterruptedException {
        ItemProducer messageProducer = new ItemProducer(propsMap());

        List
                .of(
                        new Item(3, "Samsung TVX", 400.00),
                        new Item(4, "Apple iPhone Foo", 1200.00)
                )
                .forEach(messageProducer::publishMessageSync);

//        Future<RecordMetadata> asynchronousMessageFromCode = messageProducer.publishMessageAsync(null, "Asynchronous message from code");
//        while (!asynchronousMessageFromCode.isDone());
//        Thread.sleep(100);
    }

    public void close() {
        kafkaProducer.close();
    }
}
