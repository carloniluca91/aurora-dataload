package it.luca.aurora.configuration.metadata.extract;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Function;

public class AvroExtract extends Extract {

    @JsonCreator
    public AvroExtract(@JsonProperty(JsonField.TYPE) String type,
                       @JsonProperty(JsonField.FILE_NAME_REGEX) String fileNameRegex) {

        super(type, fileNameRegex);
    }

    @Override
    protected DataFrameReader setUpReader(SparkSession sparkSession) {
        return sparkSession.read().format("avro");
    }

    @Override
    protected Function<String, Dataset<Row>> invokeReader(DataFrameReader reader) {
        return reader::load;
    }
}
