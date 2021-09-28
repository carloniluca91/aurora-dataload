package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.Map;

import static it.luca.aurora.configuration.metadata.Utils.getValueOrThrowException;
import static java.util.Objects.requireNonNull;

public class DataSourceMetadata {

    public static final String LANDING = "landing";
    public static final String SUCCESS = "success";
    public static final String FAILED = "failed";

    @Getter
    private final String id;

    private final Map<String, String> dataSourcePaths;

    @Getter
    private final EtlConfiguration etlConfiguration;

    @JsonCreator
    public DataSourceMetadata(@JsonProperty(JsonField.ID) String id,
                              @JsonProperty(JsonField.DATASOURCE_PATHS) Map<String, String> dataSourcePaths,
                              @JsonProperty(JsonField.ETL_CONFIGURATION) EtlConfiguration etlConfiguration) {

        this.id = requireNonNull(id, JsonField.ID);
        this.dataSourcePaths = requireNonNull(dataSourcePaths, JsonField.DATASOURCE_PATHS);
        this.etlConfiguration = requireNonNull(etlConfiguration, JsonField.ETL_CONFIGURATION);
    }

    public String getFileNameRegex() {

        return etlConfiguration.getExtract().getFileNameRegex();
    }

    /**
     * Get HDFS path of directory where to find input files
     * @return HDFS path of directory where to find input files
     */

    public String getLandingPath() {
        return getValueOrThrowException(dataSourcePaths, LANDING);
    }

    /**
     * Get HDFS path of directory where to move input files that have been processed successfully
     * @return HDFS path of directory where to move input files that have been processed successfully
     */

    public String getSuccessPath() {
        return getValueOrThrowException(dataSourcePaths, SUCCESS);
    }

    /**
     * Get HDFS path of directory where to move input files that have been processed unsuccessfully
     * @return HDFS path of directory where to move input files that have been processed unsuccessfully
     */

    public String getFailedPath() {
        return getValueOrThrowException(dataSourcePaths, FAILED);
    }
}
