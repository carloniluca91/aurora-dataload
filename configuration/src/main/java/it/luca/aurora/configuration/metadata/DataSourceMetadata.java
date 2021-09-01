package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class DataSourceMetadata {

    private final String id;
    private final Double version;
    private final DataSourcePaths dataSourcePaths;
    private final EtlConfiguration etlConfiguration;

    @JsonCreator
    public DataSourceMetadata(@JsonProperty(JsonField.ID) String id,
                              @JsonProperty(JsonField.VERSION) Double version,
                              @JsonProperty(JsonField.DATASOURCE_PATHS) DataSourcePaths dataSourcePaths,
                              @JsonProperty(JsonField.ETL_CONFIGURATION) EtlConfiguration etlConfiguration) {

        this.id = id;
        this.version = version;
        this.dataSourcePaths = dataSourcePaths;
        this.etlConfiguration = etlConfiguration;
    }

    public String getFileNameRegex() {

        return etlConfiguration.getExtract().getFileNameRegex();
    }
}
