package com.pagopa.scenarios;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.pagopa.datastream.EventHub;
import com.pagopa.datastream.Mongo;
import com.pagopa.deserializer.MongoStudentDeserializer;
import com.pagopa.model.MongoStudent;

public class MongoScenario implements Scenario {

    private Properties properties;
    private StreamExecutionEnvironment environment;
    private StreamTableEnvironment tableEnvironment;

    private static final String KAFKA_STREAM_TABLE = "kafka_students";
    private static final String DATA_SOURCE_NAME = "Event Hub Source";
    private static final String MONGO_STREAM_TABLE = "mongo_students";

    public MongoScenario(Properties properties, StreamExecutionEnvironment environment,
            StreamTableEnvironment tableEnvironment) {
        this.properties = properties;
        this.environment = environment;
        this.tableEnvironment = tableEnvironment;
    }

    @Override
    public void execute() throws Exception {
        EventHub<MongoStudent> studentsDataStream = new EventHub<MongoStudent>(environment,
                tableEnvironment, properties)
                .createDataSource(KafkaRecordDeserializationSchema
                        .of(new MongoStudentDeserializer()))
                .createDataStream(WatermarkStrategy.noWatermarks(), DATA_SOURCE_NAME);
        studentsDataStream.createTableFromDataStream(KAFKA_STREAM_TABLE);
        // tableEnvironment.executeSql("SELECT * FROM " + KAFKA_STREAM_TABLE).print();

        // CREATE MONGO DATA STREAM
        Mongo mongoDataStream = new Mongo(environment, tableEnvironment, properties)
                .createDataSource()
                .createDataStream(WatermarkStrategy.noWatermarks(), "MongoDB-Source");
        mongoDataStream.createTableFromDataStream(MONGO_STREAM_TABLE);
        // tableEnvironment.executeSql("SELECT * FROM " + MONGO_STREAM_TABLE).print();

        // JOIN MONGO DATA WITH EVENT HUB DATA STREAM
        tableEnvironment.executeSql("SELECT * FROM " + KAFKA_STREAM_TABLE + " s JOIN " + MONGO_STREAM_TABLE
                + " s2 ON s.statusId = s2.id").print();
    }

}
