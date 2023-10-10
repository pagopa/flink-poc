package com.pagopa.datastream;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EventHub<T> {

    private StreamExecutionEnvironment environment;
    private StreamTableEnvironment streamTableEnvironment;
    private Properties properties;

    private KafkaSource<T> source;
    private DataStream<T> dataStream;

    public EventHub(StreamExecutionEnvironment environment, StreamTableEnvironment streamTableEnvironment,
            Properties properties) {
        this.environment = environment;
        this.streamTableEnvironment = streamTableEnvironment;
        this.properties = properties;
    }

    public EventHub<T> createDataSource(KafkaRecordDeserializationSchema<T> deserializationSchema) {
        this.source = KafkaSource.<T>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setTopics(properties.getProperty("topics"))
                .setGroupId(properties.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(
                        deserializationSchema)
                .setProperty("sasl.mechanism", properties.getProperty("sasl.mechanism"))
                .setProperty("security.protocol", properties.getProperty("security.protocol"))
                .setProperty("sasl.jaas.config",
                        properties.getProperty("sasl.jaas.config"))
                .build();
        return this;
    }

    public EventHub<T> createDataStream(WatermarkStrategy<T> timestampsAndWatermarks,
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

    public DataStream<T> getDataStream() {
        return this.dataStream;
    }
}
