package com.pagopa.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@PublicEvolving
public class CosmosConnectionOptionsConstants {
        private CosmosConnectionOptionsConstants() {
        }

        public static final ConfigOption<String> SERVICE_ENDPOINT = ConfigOptions.key("service_endpoint")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("Specifies the connection uri of Cosmos.");

        public static final ConfigOption<String> KEY = ConfigOptions.key("key")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("Specifies the key for connecting to Cosmos.");
}
