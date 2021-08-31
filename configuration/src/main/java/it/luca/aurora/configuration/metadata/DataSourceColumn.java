package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

@Getter
public class DataSourceColumn {

    private final String name;
    private final DataType type;

    @JsonCreator
    public DataSourceColumn(@JsonProperty("name") String name,
                            @JsonProperty("type") String type) {

        this.name = name;
        DataType matchedDataType;
        switch (type) {

            case "string": matchedDataType = DataTypes.StringType; break;
            case "int": matchedDataType = DataTypes.IntegerType; break;
            case "double": matchedDataType = DataTypes.DoubleType; break;
            case "float": matchedDataType = DataTypes.FloatType; break;
            case "date": matchedDataType = DataTypes.DateType; break;
            case "timestamp": matchedDataType = DataTypes.TimestampType; break;
            default: throw new IllegalArgumentException(String.format("Unmatched dataType: %s", type));
        }

        this.type = matchedDataType;
    }

    public StructField toStructField() {

        return new StructField(name, type, true, Metadata.empty());
    }
}
