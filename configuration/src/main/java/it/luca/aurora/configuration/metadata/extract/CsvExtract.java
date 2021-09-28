package it.luca.aurora.configuration.metadata.extract;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@Getter
public class CsvExtract extends Extract {

    private final CsvExtractConfiguration configuration;

    @JsonCreator
    public CsvExtract(@JsonProperty(JsonField.TYPE) String type,
                      @JsonProperty(JsonField.FILE_NAME_REGEX) String fileNameRegex,
                      @JsonProperty(JsonField.CONFIGURATION) CsvExtractConfiguration configuration) {

        super(type, fileNameRegex);
        this.configuration = requireNonNull(configuration, JsonField.CONFIGURATION);
    }

    @Override
    protected DataFrameReader setUpReader(SparkSession sparkSession) {

        // Add options if present
        DataFrameReader readerMaybeWithOptions = Optional.ofNullable(configuration.getOptions()).isPresent()
                && !configuration.getOptions().isEmpty()?
                sparkSession.read().options(configuration.getOptions()) :
                sparkSession.read();

        // Add schema if present
        return Optional.ofNullable(configuration.getSchema()).isPresent()
                && !configuration.getSchema().isEmpty() ?
                readerMaybeWithOptions.schema(configuration.getInputSchemaAsStructType()) :
                readerMaybeWithOptions;
    }

    @Override
    protected Function<String, Dataset<Row>> invokeReader(DataFrameReader reader) {
        return reader::csv;
    }
}
