package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

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

        this.id = Objects.requireNonNull(id, JsonField.ID);
        this.dataSourcePaths = Objects.requireNonNull(dataSourcePaths, JsonField.DATASOURCE_PATHS);
        this.etlConfiguration = Objects.requireNonNull(etlConfiguration, JsonField.ETL_CONFIGURATION);
    }

    public String getFileNameRegex() {

        return etlConfiguration.getExtract().getFileNameRegex();
    }

    /**
     * Retrieve value of given key or throws exception
     * @param key input key
     * @return value of given key
     * @throws NoSuchElementException if given key is not present
     */

    protected String getKeyOrThrowException(String key) throws NoSuchElementException {

        if (dataSourcePaths.containsKey(key)) {
            return dataSourcePaths.get(key);
        } else {
            throw new NoSuchElementException(String.format("Key '%s' not found in %s map", key, JsonField.DATASOURCE_PATHS));
        }
    }

    /**
     * Get HDFS path of directory where to find input files
     * @return HDFS path of directory where to find input files
     */

    public String getLandingPath() {
        return getKeyOrThrowException(LANDING);
    }

    /**
     * Get HDFS path of directory where to move input files that have been processed successfully
     * @return HDFS path of directory where to move input files that have been processed successfully
     */

    public String getSuccessPath() {
        return getKeyOrThrowException(SUCCESS);
    }

    /**
     * Get HDFS path of directory where to move input files that have been processed unsuccessfully
     * @return HDFS path of directory where to move input files that have been processed unsuccessfully
     */

    public String getFailedPath() {
        return getKeyOrThrowException(FAILED);
    }
}
