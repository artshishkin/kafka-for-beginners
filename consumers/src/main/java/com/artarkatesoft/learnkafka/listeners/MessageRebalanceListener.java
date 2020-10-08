package com.artarkatesoft.learnkafka.listeners;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MessageRebalanceListener implements ConsumerRebalanceListener {

    Logger log = LoggerFactory.getLogger(MessageRebalanceListener.class);

    private final KafkaConsumer<String, String> kafkaConsumer;

    public MessageRebalanceListener(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("onPartitionsRevoked: {}", partitions);
        kafkaConsumer.commitSync();
        log.info("Offsets committed");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("onPartitionsAssigned: {}", partitions);
//        kafkaConsumer.seekToBeginning(partitions);
//        kafkaConsumer.seekToEnd(partitions);
    }
}
