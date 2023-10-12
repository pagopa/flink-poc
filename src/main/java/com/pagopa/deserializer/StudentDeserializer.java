package com.pagopa.deserializer;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.pagopa.model.Student;

public class StudentDeserializer implements KafkaDeserializationSchema<Student> {
    private ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        mapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public Student deserialize(ConsumerRecord<byte[], byte[]> record) throws IOException {
        if (record.value() != null) {
            return mapper.readValue(record.value(), Student.class);
        }
        return null;
    }

    @Override
    public TypeInformation<Student> getProducedType() {
        return TypeInformation.of(new TypeHint<Student>() {
        });
    }

    @Override
    public boolean isEndOfStream(Student nextElement) {
        return false;
    }
}
