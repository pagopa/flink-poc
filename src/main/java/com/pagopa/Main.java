package com.pagopa;

import java.io.FileReader;
import java.util.Properties;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.pagopa.scenarios.ScenariosFactory;
import com.pagopa.scenarios.ScenariosFactory.Scenarios;

public class Main {
        private static final String FILE_PATH = "src/main/resources/env.config";

        public static void main(String[] args) throws Exception {

                Properties properties = new Properties();
                properties.load(new FileReader(FILE_PATH));

                StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
                StreamTableEnvironment tableEnvironment = StreamTableEnvironment
                                .create(environment);

                ScenariosFactory.getScenario(Scenarios.COSMOS, properties, environment, tableEnvironment).execute();
        }

}