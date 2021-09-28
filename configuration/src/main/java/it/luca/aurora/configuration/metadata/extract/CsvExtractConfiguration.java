package it.luca.aurora.configuration.metadata.extract;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Slf4j
@Getter
public class CsvExtractConfiguration {

    private final Map<String, String> options;
    private final List<DataSourceColumn> schema;

    @JsonCreator
    public CsvExtractConfiguration(@JsonProperty(JsonField.OPTIONS) Map<String, String> options,
                                   @JsonProperty(JsonField.SCHEMA) List<DataSourceColumn> schema) {

        this.options = requireNonNull(options, JsonField.OPTIONS);
        this.schema = schema;
    }

    public StructType getInputSchemaAsStructType() {

        StructField[] structFields = schema.stream()
                .map(DataSourceColumn::toStructField)
                .toArray(StructField[]::new);

        log.info("Successfully parsed all of {} {}(s) to {}",
                schema.size(),
                DataSourceColumn.class.getSimpleName(),
                StructField.class.getSimpleName());

        return new StructType(structFields);
    }

}
