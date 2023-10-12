package com.pagopa.scenarios;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.pagopa.catalogs.JDBCCatalogs;
import com.pagopa.catalogs.PostgreSQL;
import com.pagopa.datastream.EventHub;
import com.pagopa.deserializer.StudentDeserializer;
import com.pagopa.model.Student;

public class PostgreSQLScenario implements Scenario {
    private Properties properties;
    private StreamExecutionEnvironment environment;
    private StreamTableEnvironment tableEnvironment;

    private static final String KAFKA_STREAM_TABLE = "kafka_students";
    private static final String DATA_SOURCE_NAME = "Event Hub Source";
    private static final String POSTGRESQL_CATALOG = "postgresqlCatalog";
    private static final String POSTGRESQL_STATUS = "status";

    public PostgreSQLScenario(Properties properties, StreamExecutionEnvironment environment,
            StreamTableEnvironment tableEnvironment) {
        this.properties = properties;
        this.environment = environment;
        this.tableEnvironment = tableEnvironment;
    }

    @Override
    public void execute() throws Exception {
        // CREATE EVENT HUB DATA STREAM
        EventHub<Student> studentsDataStream = new EventHub<Student>(environment,
                tableEnvironment, properties)
                .createDataSource(KafkaRecordDeserializationSchema
                        .of(new StudentDeserializer()))
                .createDataStream(WatermarkStrategy.noWatermarks(), DATA_SOURCE_NAME);
        studentsDataStream.createTableFromDataStream(KAFKA_STREAM_TABLE);
        // tableEnvironment.executeSql("SELECT * FROM " + KAFKA_STREAM_TABLE).print();

        // CREATE POSTGRESQL CATALOG + LOAD KAFKA STREAM TABLE
        JDBCCatalogs postgreSQL = new PostgreSQL(tableEnvironment, properties);
        postgreSQL.registerJdbcCatalog(POSTGRESQL_CATALOG);
        studentsDataStream.createTableFromDataStream(KAFKA_STREAM_TABLE);

        // // JOIN POSTGRESQL DATA WITH EVENT HUB DATA STREAM
        tableEnvironment.executeSql(
                "SELECT * FROM " + KAFKA_STREAM_TABLE + " s JOIN " + POSTGRESQL_STATUS
                        + " t ON s.statusId = t.id")
                .print();
    }
}
