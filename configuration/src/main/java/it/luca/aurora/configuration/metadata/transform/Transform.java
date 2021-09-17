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
public class Transform {

    private final String DATASET_CLASS = Dataset.class.getSimpleName();

    @Getter
    private final List<String> filters;

    @Getter
    private final List<String> transformations;

    @Getter
    private final List<String> dropDuplicates;

    @Getter
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
     * Optionally remove duplicates from given dataset
     * @param input input {@link Dataset}
     * @return input dataset with duplicates removed according to stated columns (if any), the input itself otherwise
     */

    public Dataset<Row> maybeRemoveDuplicates(Dataset<Row> input) {

        Dataset<Row> output;
        if (nullOrEmptyList(dropDuplicates)) {
            log.info("No duplicates will be removed from input {}", DATASET_CLASS);
            output = input;
        } else {
            log.info("Removing duplicates from input {} according to columns: {}",
                    DATASET_CLASS, String.join("|", dropDuplicates));
            output = input.dropDuplicates(dropDuplicates.toArray(new String[0]));
        }

        return output;
    }

    /**
     * Optionally drop some columns from given dataset
     * @param input input {@link Dataset}
     * @return input dataset with stated columns dropped (if any), the input itself otherwise
     */

    public Dataset<Row> maybeDropColumns(Dataset<Row> input) {

        Dataset<Row> output;
        if (nullOrEmptyList(dropColumns)) {
            log.info("No columns to drop from input {}", DATASET_CLASS);
            output = input;
        } else {
            log.info("Dropping following columns from input {}: {}",
                    DATASET_CLASS, String.join("|", dropColumns));
            output = input.drop(dropColumns.toArray(new String[0]));
        }

        return output;
    }
}
