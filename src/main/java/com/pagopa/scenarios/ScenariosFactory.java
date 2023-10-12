package com.pagopa.scenarios;

import java.util.Properties;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ScenariosFactory {
    public enum Scenarios {
        POSTGRESQL,
        MONGO,
        COSMOS
    }

    public static Scenario getScenario(Scenarios scenario, Properties properties,
            StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        switch (scenario) {
            case POSTGRESQL:
                return new PostgreSQLScenario(properties, environment, tableEnvironment);
            case MONGO:
                return new MongoScenario(properties, environment, tableEnvironment);
            case COSMOS:
                return new CosmosScenario(properties, environment, tableEnvironment);
            default:
                return null;
        }
    }
}
