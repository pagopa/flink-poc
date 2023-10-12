package com.pagopa.datastream;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.pagopa.deserializer.StatusDeserializer;
import com.pagopa.model.Status;

public class Mongo {
    private StreamExecutionEnvironment environment;
    private StreamTableEnvironment streamTableEnvironment;
    private Properties properties;

    private MongoSource<Status> source;
    private DataStream<Status> dataStream;

    public Mongo(StreamExecutionEnvironment environment, StreamTableEnvironment streamTableEnvironment,
            Properties properties) {
        this.environment = environment;
        this.streamTableEnvironment = streamTableEnvironment;
        this.properties = properties;
    }

    public Mongo createDataSource() {
        this.source = MongoSource.<Status>builder()
                .setUri(properties.getProperty(
                        "mongo.uri"))
                .setDatabase(properties.getProperty(
                        "mongo.database"))
                .setCollection(properties.getProperty(
                        "mongo.collection"))
                .setPartitionStrategy(PartitionStrategy.SAMPLE)
                .setNoCursorTimeout(true)
                .setDeserializationSchema(
                        new StatusDeserializer())
                .build();
        return this;
    }

    public Mongo createDataStream(WatermarkStrategy<Status> timestampsAndWatermarks,
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
}
