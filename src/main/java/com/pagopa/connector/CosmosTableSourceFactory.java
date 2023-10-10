package com.pagopa.connector;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class CosmosTableSourceFactory implements DynamicTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "CosmosDB";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(CosmosConnectionOptionsConstants.SERVICE_ENDPOINT);
        requiredOptions.add(CosmosConnectionOptionsConstants.KEY);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context ctx) {
        final FactoryUtil.TableFactoryHelper factoryHelper = FactoryUtil.createTableFactoryHelper(this, ctx);
        ReadableConfig config = factoryHelper.getOptions();
        factoryHelper.validate();

        CosmosConnectionOptions cosmosConnectionOptions = new CosmosConnectionOptions(config.get(
                CosmosConnectionOptionsConstants.SERVICE_ENDPOINT),
                config.get(
                        CosmosConnectionOptionsConstants.KEY));
        return new CosmosTableSource(cosmosConnectionOptions);
    }
}