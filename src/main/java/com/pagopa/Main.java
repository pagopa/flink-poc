package com.pagopa;

import java.io.FileReader;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.pagopa.datastream.EventHub;

public class Main {
        private static final String FILE_PATH = "src/main/resources/env.config";
        private static final String KAFKA_STREAM_TABLE = "students";
        private static final String DATA_SOURCE_NAME = "Event Hub Source";
        // private static final String POSTGRESQL_CATALOG = "postgresqlCatalog";
        // private static final String COSMOS_CATALOG = "cosmosCatalog";
        // private static final String MONGO_STREAM_TABLE = "mongo_students";

        public static void main(String[] args) throws Exception {

                Properties properties = new Properties();
                properties.load(new FileReader(FILE_PATH));

                StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
                StreamTableEnvironment tableEnvironment = StreamTableEnvironment
                                .create(environment);

                // CREATE EVENT HUB DATA STREAM
                EventHub<ObjectNode> studentsDataStream = new EventHub<ObjectNode>(environment,
                                tableEnvironment, properties)
                                .createDataSource(KafkaRecordDeserializationSchema
                                                .of(new JSONKeyValueDeserializationSchema(false)))
                                .createDataStream(WatermarkStrategy.noWatermarks(), DATA_SOURCE_NAME);
                studentsDataStream.createTableFromDataStream(KAFKA_STREAM_TABLE);

                /*
                 * CREATE MONGO DATA STREAM
                 * Mongo mongoDataStream = new Mongo(environment, tableEnvironment, properties)
                 * .createDataSource()
                 * .createDataStream(WatermarkStrategy.noWatermarks(), "MongoDB-Source");
                 * mongoDataStream.createTableFromDataStream(MONGO_STREAM_TABLE);
                 * tableEnvironment.executeSql("SELECT * FROM " + MONGO_STREAM_TABLE);
                 * 
                 * JOIN MONGO DATA WITH EVENT HUB DATA STREAM
                 * tableEnvironment.executeSql("SELECT * FROM " + KAFKA_STREAM_TABLE + " s JOIN
                 * " + MONGO_STREAM_TABLE
                 * + " s2 ON s.status_id = s2.statusId").print();
                 */

                /*
                 * CREATE POSTGRESQL CATALOG
                 * JDBCCatalogs postgreSQL = new PostgreSQL(tableEnvironment, properties);
                 * postgreSQL.registerJdbcCatalog(POSTGRESQL_CATALOG);
                 * 
                 * JOIN POSTGRESQL DATA WITH EVENT HUB DATA STREAM
                 * tableEnvironment.executeSql("SELECT * FROM " + KAFKA_STREAM_TABLE + " s JOIN
                 * status s2 ON
                 * s.status_id = s2.id").print();
                 */

                /*
                 * CREATE CUSTOM COSMOS CONNECTOR
                 * String cosmosEndpoint = properties.getProperty("cosmos.endpoint");
                 * String cosmosKey = properties.getProperty("cosmos.key");
                 * tableEnvironment.executeSql(
                 * "CREATE TABLE T (id STRING, valore STRING) WITH ('connector' = 'CosmosDB', 'service_endpoint' = '"
                 * + cosmosEndpoint + "', 'key' = '" + cosmosKey + "');");
                 * tableEnvironment.executeSql("SELECT * FROM T;").print();
                 * 
                 * JOIN COSMOS DATA WITH EVENT HUB DATA STREAM
                 * tableEnvironment.executeSql("SELECT * FROM " + KAFKA_STREAM_TABLE + " s JOIN
                 * T ON
                 * s.status_id = T.id").print();
                 */

                environment.execute("Job");

        }
}