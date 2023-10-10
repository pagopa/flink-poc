package com.pagopa.connector;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

public class CosmosTableSource implements ScanTableSource {
  private final CosmosConnectionOptions cosmosConnectionOptions;

  public CosmosTableSource(
      CosmosConnectionOptions cosmosConnectionOptions) {
    this.cosmosConnectionOptions = cosmosConnectionOptions;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {
    boolean bounded = true;
    final CosmosSource source = new CosmosSource(this.cosmosConnectionOptions);
    return SourceFunctionProvider.of(source, bounded);
  }

  @Override
  public DynamicTableSource copy() {
    return new CosmosTableSource(cosmosConnectionOptions);
  }

  @Override
  public String asSummaryString() {
    return "Cosmos Table Source";
  }
}