package it.luca.aurora.configuration.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Optional;

@Slf4j
@Getter
public class Transform {

    private final List<String> filters;
    private final List<String> transformations;
    private final List<String> dropDuplicates;
    private final List<String> dropColumns;

    @JsonCreator
    public Transform(@JsonProperty(JsonField.FILTERS) List<String> filters,
                     @JsonProperty(JsonField.TRANSFORMATIONS) List<String> transformations,
                     @JsonProperty(JsonField.DROP_DUPLICATES) List<String> dropDuplicates,
                     @JsonProperty(JsonField.DROP_COLUMNS) List<String> dropColumns) {

        this.filters = filters;
        this.transformations = transformations;
        this.dropDuplicates = dropDuplicates;
        this.dropColumns = dropColumns;
    }

    protected boolean nullOrEmptyList(List<?> list) {

        return !Optional.ofNullable(list).isPresent() || list.isEmpty();
    }

    /**
     * Optionally remove duplicates and drop columns from input dataset
     * @param input input {@link Dataset}
     * @return input dataset itself or the input dataset with either dropped duplicates or dropped columns or both
     */

    public Dataset<Row> maybeDropDuplicatesAndColumns(Dataset<Row> input) {

        String DATASET_CLASS = Dataset.class.getSimpleName();
        Dataset<Row> maybeDroppedDuplicatesDataset;
        if (nullOrEmptyList(dropDuplicates)) {
            log.info("No duplicates will be removed from input {}", DATASET_CLASS);
            maybeDroppedDuplicatesDataset = input;
        } else {
            log.info("Removing duplicates from input {} according to columns: {}",
                    DATASET_CLASS, String.join("|", dropDuplicates));
            maybeDroppedDuplicatesDataset = input.dropDuplicates(dropDuplicates.toArray(new String[0]));
        }

        Dataset<Row> maybeDroppedDuplicatesAndColumnsDataset;
        if (nullOrEmptyList(dropColumns)) {
            log.info("No columns to drop from input {}", DATASET_CLASS);
            maybeDroppedDuplicatesAndColumnsDataset = maybeDroppedDuplicatesDataset;
        } else {
            log.info("Dropping following columns from input {}: {}",
                    DATASET_CLASS, String.join("|", dropColumns));
            maybeDroppedDuplicatesAndColumnsDataset = maybeDroppedDuplicatesDataset.drop(dropColumns.toArray(new String[0]));
        }

        return maybeDroppedDuplicatesAndColumnsDataset;
    }
}
