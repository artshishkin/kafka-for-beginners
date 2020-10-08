package com.artarkatesoft.learnkafka.serializers;

import com.artarkatesoft.learnkafka.domain.Item;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemSerializer implements Serializer<Item> {

    private static Logger log = LoggerFactory.getLogger(ItemSerializer.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Item item) {
        try {
            log.info("Serializing {}", item);
            return objectMapper.writeValueAsBytes(item);
        } catch (JsonProcessingException e) {
            log.error("Error while serializing {}", item, e);
        }
        return new byte[0];
    }
}
