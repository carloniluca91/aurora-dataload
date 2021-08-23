package it.luca.aurora.core.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.apache.spark.sql.SaveMode;

import java.util.List;

@Getter
public class DataSourceMetadata {

    private final String id;
    private final Double version;
    private final DataSourcePaths paths;
    private final String fileNameRegex;
    private final DataSourceSchema schema;
    private final List<String> filters;
    private final List<DataSourceTransformation> trasformations;
    private final String outputTable;
    private final SaveMode saveMode;
    private final PartitionStrategy partitionStrategy;

    @JsonCreator
    public DataSourceMetadata(@JsonProperty("id") String id,
                              @JsonProperty("version") Double version,
                              @JsonProperty("paths") DataSourcePaths paths,
                              @JsonProperty("fileNameRegex") String fileNameRegex,
                              @JsonProperty("schema") DataSourceSchema schema,
                              @JsonProperty("filters") List<String> filters,
                              @JsonProperty("trasformations") List<DataSourceTransformation> trasformations,
                              @JsonProperty("outputTable") String outputTable,
                              @JsonProperty("saveMode") String saveMode,
                              @JsonProperty("partitionStrategy") PartitionStrategy partitionStrategy) {

        this.id = id;
        this.version = version;
        this.paths = paths;
        this.fileNameRegex = fileNameRegex;
        this.schema = schema;
        this.filters = filters;
        this.trasformations = trasformations;
        this.outputTable = outputTable;
        this.saveMode = SaveMode.valueOf(saveMode);
        this.partitionStrategy = partitionStrategy;
    }
}
