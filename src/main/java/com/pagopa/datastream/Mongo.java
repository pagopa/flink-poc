package com.pagopa.datastream;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

public class Mongo {
    private StreamExecutionEnvironment environment;
    private StreamTableEnvironment streamTableEnvironment;
    private Properties properties;

    private MongoSource<ObjectNode> source;
    private DataStream<ObjectNode> dataStream;

    public Mongo(StreamExecutionEnvironment environment, StreamTableEnvironment streamTableEnvironment,
            Properties properties) {
        this.environment = environment;
        this.streamTableEnvironment = streamTableEnvironment;
        this.properties = properties;
    }

    public Mongo createDataSource() {
        this.source = MongoSource.<ObjectNode>builder()
                .setUri(properties.getProperty(
                        "mongo.uri"))
                .setDatabase(properties.getProperty(
                        "mongo.database"))
                .setCollection(properties.getProperty(
                        "mongo.collection"))
                .setPartitionStrategy(PartitionStrategy.SAMPLE)
                .setNoCursorTimeout(true)
                .setDeserializationSchema(
                        new MongoJsonDeserializationSchema())
                .build();
        return this;
    }

    public Mongo createDataStream(WatermarkStrategy<ObjectNode> timestampsAndWatermarks,
            String sourceName) throws Exception {
        if (this.source == null) {
            throw new Exception("Source cannot be null, please call getDataSource() before calling getDataStream()");
        }
        this.dataStream = this.environment.fromSource(this.source,
                timestampsAndWatermarks,
                sourceName);
        this.dataStream.print();
        return this;
    }

    public void createTableFromDataStream(String tableName)
            throws Exception {
        if (this.dataStream == null) {
            throw new Exception(
                    "DataStream cannot be null, please call getDataSource() and  getDataStream() before calling getTableFromDataStream()");
        }
        Table table = streamTableEnvironment.fromDataStream(this.dataStream);
        streamTableEnvironment.createTemporaryView(tableName, table);
    }

    private static class MongoJsonDeserializationSchema
            implements MongoDeserializationSchema<ObjectNode> {
        private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

        @Override
        public ObjectNode deserialize(BsonDocument document) {
            ObjectNode objectNode = mapper.createObjectNode();
            String json = document.toJson(JsonWriterSettings
                    .builder()
                    .outputMode(JsonMode.RELAXED)
                    .build());

            try {
                objectNode = mapper.readValue(json, ObjectNode.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return objectNode;
        }

        public TypeInformation<ObjectNode> getProducedType() {
            return getForClass(ObjectNode.class);
        }
    }
}
