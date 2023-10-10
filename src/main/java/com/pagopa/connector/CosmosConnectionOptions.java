package com.pagopa.connector;

import java.io.Serializable;

import org.apache.flink.annotation.PublicEvolving;

@PublicEvolving
public class CosmosConnectionOptions implements Serializable {

    private final String endpoint;
    private final String key;

    public CosmosConnectionOptions(String endpoint, String key) {
        this.endpoint = endpoint;
        this.key = key;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getKey() {
        return key;
    }
}