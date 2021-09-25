package it.luca.aurora.configuration.metadata.extract;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;
import java.util.function.Function;

@Slf4j
@Getter
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        property = JsonField.TYPE,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CsvExtract.class, name = Extract.CSV),
        @JsonSubTypes.Type(value = AvroExtract.class, name = Extract.AVRO)
})
public abstract class Extract {

    public static final String AVRO = "AVRO";
    public static final String CSV = "CSV";

    protected final String type;
    protected final String fileNameRegex;

    public Extract(String type, String fileNameRegex) {

        this.type = Objects.requireNonNull(type, JsonField.TYPE);
        this.fileNameRegex = Objects.requireNonNull(fileNameRegex, JsonField.FILE_NAME_REGEX);
    }

    protected abstract DataFrameReader setUpReader(SparkSession sparkSession);

    protected abstract Function<String, Dataset<Row>> invokeReader(DataFrameReader reader);

    public Dataset<Row> read(SparkSession sparkSession, Path path) {

        String fileName = path.getName();
        log.info("Starting to read input file {}", fileName);
        Dataset<Row> dataset = invokeReader(setUpReader(sparkSession)).apply(path.toString());
        log.info("Successfully read input file {}. Schema:\n\n{}", fileName, dataset.schema().treeString());
        return dataset;
    }
}
