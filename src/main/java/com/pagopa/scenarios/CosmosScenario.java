package com.pagopa.scenarios;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.pagopa.datastream.EventHub;
import com.pagopa.deserializer.MongoStudentDeserializer;
import com.pagopa.model.MongoStudent;

public class CosmosScenario implements Scenario {

        private Properties properties;
        private StreamExecutionEnvironment environment;
        private StreamTableEnvironment tableEnvironment;

        private static final String KAFKA_STREAM_TABLE = "kafka_students";
        private static final String DATA_SOURCE_NAME = "Event Hub Source";

        public CosmosScenario(Properties properties, StreamExecutionEnvironment environment,
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

                String cosmosEndpoint = properties.getProperty("cosmos.endpoint");
                String cosmosKey = properties.getProperty("cosmos.key");
                tableEnvironment.executeSql(
                                "CREATE TABLE T (id INTEGER, status_value STRING) WITH ('connector' = 'CosmosDB', 'service_endpoint' = '"
                                                + cosmosEndpoint + "', 'key' = '" + cosmosKey + "');");
                // tableEnvironment.executeSql("SELECT * FROM T;").print();

                // JOIN COSMOS DATA WITH EVENT HUB DATA STREAM
                tableEnvironment.executeSql("SELECT * FROM " + KAFKA_STREAM_TABLE + " s JOIN T ON s.statusId = T.id")
                                .print();
        }

}
