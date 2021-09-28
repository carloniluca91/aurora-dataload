package it.luca.aurora.configuration.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DataSourcesWrapper {

    public static final String DATASOURCES = "dataSources";

    private final Map<String, String> dataSources;

    @JsonCreator
    public DataSourcesWrapper(@JsonProperty(DATASOURCES) Map<String, String> dataSources) {

        this.dataSources = requireNonNull(dataSources, DATASOURCES);
    }

    /**
     * Return instance of {@link DataSource} with given id if it exists
     * @param id dataSource id
     * @return {@link DataSource}
     * @throws UnExistingDataSourceException if no dataSource is found
     */

    public DataSource getDataSourceWithId(String id) throws UnExistingDataSourceException {

        if (!dataSources.containsKey(id)) {
            throw new UnExistingDataSourceException(id);
        } else {
            return new DataSource(id, dataSources.get(id));
        }
    }
}
