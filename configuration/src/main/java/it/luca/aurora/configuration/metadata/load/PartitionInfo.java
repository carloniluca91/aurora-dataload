package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

import static java.util.Objects.requireNonNull;

@Getter
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        property = JsonField.TYPE,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = FileNameRegexInfo.class, name = PartitionInfo.FILE_NAME_REGEX),
        @JsonSubTypes.Type(value = ColumnExpressionInfo.class, name = PartitionInfo.COLUMN_EXPRESSION)
})
public abstract class PartitionInfo {

    public static final String COLUMN_EXPRESSION = "COLUMN_EXPRESSION";
    public static final String FILE_NAME_REGEX = "FILE_NAME_REGEX";

    protected final String type;
    protected final String columnName;

    public PartitionInfo(String type, String columnName) {

        this.type = requireNonNull(type, JsonField.TYPE);
        this.columnName = requireNonNull(columnName, JsonField.COLUMN_NAME);
    }

}
