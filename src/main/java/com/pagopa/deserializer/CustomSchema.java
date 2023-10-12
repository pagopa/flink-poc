package com.pagopa.deserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CustomSchema implements KafkaDeserializationSchema<Map<String, Object>> {

    private ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        mapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public Map<String, Object> deserialize(ConsumerRecord<byte[], byte[]> record) throws IOException {
        Map<String, Object> t = new HashMap<String, Object>();

        if (record.value() != null) {
            t = mapper.readValue(record.value(), Map.class);
        }
        return t;
    }

    @Override
    public TypeInformation<Map<String, Object>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, Object>>() {
        });
    }

    @Override
    public boolean isEndOfStream(Map<String, Object> nextElement) {
        return false;
    }
}