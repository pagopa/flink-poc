package com.pagopa.deserializer;

import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.pagopa.model.Status;

public class StatusDeserializer
        implements MongoDeserializationSchema<Status> {
    private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    @Override
    public Status deserialize(BsonDocument document) {
        Status status = new Status();
        String json = document.toJson(JsonWriterSettings
                .builder()
                .outputMode(JsonMode.RELAXED)
                .build());

        try {
            Map<String, Object> map = mapper.readValue(json, Map.class);
            status.setId(Integer.valueOf(map.get("id").toString()));
            status.setValue(map.get("value").toString());
        } catch (JsonProcessingException e) {

        }
        return status;
    }

    public TypeInformation<Status> getProducedType() {
        return TypeInformation.of(new TypeHint<Status>() {
        });
    }
}
