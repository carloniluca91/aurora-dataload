package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.Objects;

@Getter
public class DataSourceMetadata {

    private final String id;
    private final DataSourcePaths dataSourcePaths;
    private final EtlConfiguration etlConfiguration;

    @JsonCreator
    public DataSourceMetadata(@JsonProperty(JsonField.ID) String id,
                              @JsonProperty(JsonField.DATASOURCE_PATHS) DataSourcePaths dataSourcePaths,
                              @JsonProperty(JsonField.ETL_CONFIGURATION) EtlConfiguration etlConfiguration) {

        this.id = Objects.requireNonNull(id, JsonField.ID);
        this.dataSourcePaths = Objects.requireNonNull(dataSourcePaths, JsonField.DATASOURCE_PATHS);
        this.etlConfiguration = Objects.requireNonNull(etlConfiguration, JsonField.ETL_CONFIGURATION);
    }

    public String getFileNameRegex() {

        return etlConfiguration.getExtract().getFileNameRegex();
    }
}
