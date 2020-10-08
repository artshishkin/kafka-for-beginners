package com.artarkatesoft.learnkafka.listeners;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MessageRebalanceSeekListener implements ConsumerRebalanceListener {

    Logger log = LoggerFactory.getLogger(MessageRebalanceSeekListener.class);

    private final KafkaConsumer<String, String> kafkaConsumer;
    public static String serializationFilePath = "logs/offset.ser";

    public MessageRebalanceSeekListener(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        Path filePath = Path.of(serializationFilePath);
        Path dirPath = filePath.getParent();
        try {
            Files.createDirectories(dirPath);
        } catch (IOException e) {
            log.error("Can not create directory {}", dirPath, e);
        }

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

        Map<TopicPartition, OffsetAndMetadata> offsetMap = readOffsetSerializationFile();
        log.info("Offset map is {}", offsetMap);
        if (!offsetMap.isEmpty()) {
            partitions.forEach(partition -> {
                kafkaConsumer.seek(partition, offsetMap.get(partition));
            });
        }

    }

    private Map<TopicPartition, OffsetAndMetadata> readOffsetSerializationFile() {

        ObjectInputStream objectInputStream = null;
        BufferedInputStream bufferedInputStream = null;
        FileInputStream fileInputStream = null;

        try {
            fileInputStream = new FileInputStream(serializationFilePath);
            bufferedInputStream = new BufferedInputStream(fileInputStream);
            objectInputStream = new ObjectInputStream(bufferedInputStream);
            return (Map<TopicPartition, OffsetAndMetadata>) objectInputStream.readObject();
        } catch (Exception e) {
            log.error("Exception occurred while reading a file", e);
            return new HashMap<>();
        } finally {
//            List<Closeable> closeableList = Arrays.asList(objectInputStream, bufferedInputStream, fileInputStream);
//            closeableList.forEach(this::checkAndClose);
            checkAndClose(fileInputStream);
            checkAndClose(bufferedInputStream);
            checkAndClose(objectInputStream);
        }
    }

    private void checkAndClose(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                log.error("Exception while closing input stream {}", closeable, e);
            }
        }
    }
}
