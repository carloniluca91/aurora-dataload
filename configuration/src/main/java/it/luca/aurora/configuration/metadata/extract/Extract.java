package it.luca.aurora.configuration.metadata.extract;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Function;

@Slf4j
@Getter
@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        property = JsonField.TYPE,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CsvExtract.class, name = "csv")
})
public abstract class Extract {

    protected final String type;
    protected final String fileNameRegex;

    protected abstract DataFrameReader setUpReader(SparkSession sparkSession);

    protected abstract Function<String, Dataset<Row>> invokeReader(DataFrameReader reader);

    public Dataset<Row> read(SparkSession sparkSession, Path path) {

        String filePath = path.toString();
        log.info("Starting to read input file {}", filePath);
        Dataset<Row> dataset = invokeReader(setUpReader(sparkSession)).apply(filePath);
        log.info("Successfully read input file {}", filePath);
        return dataset;
    }
}
