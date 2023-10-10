package com.pagopa.connector;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;

public class CosmosSource extends RichSourceFunction<RowData> {

    private transient volatile boolean running = true;
    private CosmosClient cosmosClient;
    private final CosmosConnectionOptions cosmosConnectionOptions;
    private CosmosDatabase database;
    private CosmosContainer container;

    public CosmosSource(CosmosConnectionOptions cosmosConnectionOptions) {
        this.cosmosConnectionOptions = cosmosConnectionOptions;
    }

    private void connect() throws Exception {
        this.cosmosClient = new CosmosClientBuilder()
                .endpoint(this.cosmosConnectionOptions.getEndpoint())
                .key(this.cosmosConnectionOptions.getKey())
                .buildClient();
    }

    private void createDatabaseIfNotExists() throws Exception {
        CosmosDatabaseResponse cosmosDatabaseResponse = this.cosmosClient.createDatabaseIfNotExists("cosmos-flink-poc");
        this.database = this.cosmosClient.getDatabase(cosmosDatabaseResponse.getProperties().getId());
    }

    private void createContainerIfNotExists() throws Exception {
        CosmosContainerProperties containerProperties = new CosmosContainerProperties("status", "/id");

        CosmosContainerResponse cosmosContainerResponse = database.createContainerIfNotExists(containerProperties,
                ThroughputProperties.createManualThroughput(400));
        this.container = database.getContainer(cosmosContainerResponse.getProperties().getId());
    }

    private CosmosPagedIterable<JsonNode> queryItems() {
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);

        CosmosPagedIterable<JsonNode> results = container.queryItems(
                "SELECT * FROM status", queryOptions,
                JsonNode.class);
        return results;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        connect();
        createDatabaseIfNotExists();
        createContainerIfNotExists();
        CosmosPagedIterable<JsonNode> queryItems = queryItems();
        GenericRowData rowData = new GenericRowData(2);
        if (queryItems.iterator().hasNext()) {
            JsonNode queryItem = queryItems.iterator().next();
            Iterator<Entry<String, JsonNode>> iterator = queryItem.fields();
            int j = 0;
            while (iterator.hasNext()) {
                Entry<String, JsonNode> entry = iterator.next();
                if (!entry.getKey().startsWith("_")) {
                    rowData.setField(j, StringData.fromString(entry.getValue().toString()));
                    j++;
                }
            }
        }

        ctx.collect(rowData);
        while (running) {
            Thread.sleep(30000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
        this.cosmosClient.close();
    }
}