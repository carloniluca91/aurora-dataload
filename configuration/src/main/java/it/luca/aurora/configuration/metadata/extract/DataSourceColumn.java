package it.luca.aurora.configuration.metadata.extract;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import static java.util.Objects.requireNonNull;

@Getter
public class DataSourceColumn {

    private final String name;
    private final DataType type;

    @JsonCreator
    public DataSourceColumn(@JsonProperty(JsonField.NAME) String name,
                            @JsonProperty(JsonField.TYPE) String type) {

        this.name = requireNonNull(name, JsonField.NAME);
        DataType matchedDataType;
        switch (requireNonNull(type, JsonField.TYPE)) {

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
