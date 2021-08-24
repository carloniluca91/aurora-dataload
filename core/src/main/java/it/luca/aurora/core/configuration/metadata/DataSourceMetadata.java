package it.luca.aurora.core.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

@Slf4j
@Getter
public class DataSourceMetadata {

    private final String id;
    private final Double version;
    private final DataSourcePaths dataSourcePaths;
    private final String fileNameRegex;
    private final List<DataSourceColumn> inputSchema;
    private final List<String> filters;
    private final List<DataSourceTransformation> trasformations;
    private final String outputTable;
    private final SaveMode saveMode;
    private final PartitionStrategy partitionStrategy;

    @JsonCreator
    public DataSourceMetadata(@JsonProperty("id") String id,
                              @JsonProperty("version") Double version,
                              @JsonProperty("dataSourcePaths") DataSourcePaths dataSourcePaths,
                              @JsonProperty("fileNameRegex") String fileNameRegex,
                              @JsonProperty("inputSchema") List<DataSourceColumn> inputSchema,
                              @JsonProperty("filters") List<String> filters,
                              @JsonProperty("trasformations") List<DataSourceTransformation> trasformations,
                              @JsonProperty("outputTable") String outputTable,
                              @JsonProperty("saveMode") String saveMode,
                              @JsonProperty("partitionStrategy") PartitionStrategy partitionStrategy) {

        this.id = id;
        this.version = version;
        this.dataSourcePaths = dataSourcePaths;
        this.fileNameRegex = fileNameRegex;
        this.inputSchema = inputSchema;
        this.filters = filters;
        this.trasformations = trasformations;
        this.outputTable = outputTable;
        this.saveMode = SaveMode.valueOf(saveMode);
        this.partitionStrategy = partitionStrategy;
    }

    public StructType getInputSchemaAsStructType() {

        StructField[] structFields = inputSchema.stream()
                .map(DataSourceColumn::toStructField)
                .toArray(StructField[]::new);

        log.info("Successfully parsed all of {} {}(s) to {}",
                inputSchema.size(),
                DataSourceColumn.class.getSimpleName(),
                StructField.class.getSimpleName());

        return new StructType(structFields);
    }
}
