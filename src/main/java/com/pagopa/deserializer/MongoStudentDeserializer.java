package com.pagopa.deserializer;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.pagopa.model.MongoStudent;

public class MongoStudentDeserializer implements KafkaDeserializationSchema<MongoStudent> {
    private ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        mapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public MongoStudent deserialize(ConsumerRecord<byte[], byte[]> record) {
        if (record.value() != null) {
            try {
                return mapper.readValue(record.value(), MongoStudent.class);
            } catch (IOException e) {

            }
        }
        return null;
    }

    @Override
    public TypeInformation<MongoStudent> getProducedType() {
        return TypeInformation.of(new TypeHint<MongoStudent>() {
        });
    }

    @Override
    public boolean isEndOfStream(MongoStudent nextElement) {
        return false;
    }
}
