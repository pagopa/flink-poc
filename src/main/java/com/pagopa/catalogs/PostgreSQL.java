package com.pagopa.catalogs;

import java.util.Properties;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.pagopa.model.PropertiesConstants;

public class PostgreSQL implements JDBCCatalogs {
    private StreamTableEnvironment streamTableEnvironment;
    private Properties properties;

    public PostgreSQL(StreamTableEnvironment streamTableEnvironment, Properties properties) {
        this.streamTableEnvironment = streamTableEnvironment;
        this.properties = properties;
    }

    public void registerJdbcCatalog(String catalogName) {
        JdbcCatalog catalog = new JdbcCatalog(catalogName,
                this.properties.getProperty(
                        PropertiesConstants.POSTGRESQL_DEFAULT_DATABASE),
                this.properties.getProperty(
                        PropertiesConstants.POSTGRESQL_USERNAME),
                this.properties.getProperty(
                        PropertiesConstants.POSTGRESQL_PASSWORD),
                this.properties.getProperty(PropertiesConstants.POSTGRESQL_BASE_URL));
        this.streamTableEnvironment.registerCatalog(catalogName, catalog);
        this.streamTableEnvironment.useCatalog(catalogName);
    }
}
