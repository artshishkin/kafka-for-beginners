package com.artarkatesoft.learnkafka.deserializers;

import com.artarkatesoft.learnkafka.domain.Item;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ItemDeserializer implements Deserializer<Item> {

    private Logger log = LoggerFactory.getLogger(ItemDeserializer.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Item deserialize(String topic, byte[] data) {
        try {
            Item item = objectMapper.readValue(data, Item.class);
            log.info("De-serialized item: {}", item);
            return item;
        } catch (IOException e) {
            log.error("Exception while de-serializing item", e);
        }
        return null;
    }
}
