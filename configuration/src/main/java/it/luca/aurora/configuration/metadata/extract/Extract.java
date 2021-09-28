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

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@Slf4j
@Getter
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        property = JsonField.TYPE,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = AvroExtract.class, name = Extract.AVRO),
        @JsonSubTypes.Type(value = CsvExtract.class, name = Extract.CSV)
})
public abstract class Extract {

    public static final String AVRO = "AVRO";
    public static final String CSV = "CSV";

    protected final String type;
    protected final String fileNameRegex;

    public Extract(String type, String fileNameRegex) {

        this.type = requireNonNull(type, JsonField.TYPE);
        this.fileNameRegex = requireNonNull(fileNameRegex, JsonField.FILE_NAME_REGEX);
    }

    /**
     * Set up a {@link DataFrameReader} depending on represented class instance
     * @param sparkSession {@link SparkSession}
     * @return instance of {@link DataFrameReader}
     */

    protected abstract DataFrameReader setUpReader(SparkSession sparkSession);

    /**
     * Define a function that, given an input {@link DataFrameReader} and string representing an HDFS path with data to be read, returns a {@link Dataset}
     * @param reader instance of {@link DataFrameReader} (created using {@link Extract#setUpReader(SparkSession)}})
     * @return {@link Function} that, if applied to a string, returns a {@link Dataset}
     */

    protected abstract Function<String, Dataset<Row>> invokeReader(DataFrameReader reader);

    /**
     * Read data at given HDFS path using a {@link SparkSession}
     * @param sparkSession {@link SparkSession} to be used for reading
     * @param path HDFS path with data to be read
     * @return {@link Dataset}
     */

    public Dataset<Row> read(SparkSession sparkSession, Path path) {

        String fileName = path.getName();
        log.info("Starting to read input file {}", fileName);
        Dataset<Row> dataset = invokeReader(setUpReader(sparkSession)).apply(path.toString());
        log.info("Successfully read input file {}. Schema:\n\n{}", fileName, dataset.schema().treeString());
        return dataset;
    }
}
