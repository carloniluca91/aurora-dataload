package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

import java.util.Objects;

@Getter
public class ColumnExpressionInfo extends PartitionInfo {

    private final String columnExpression;

    @JsonCreator
    public ColumnExpressionInfo(@JsonProperty(JsonField.TYPE) String type,
                                @JsonProperty(JsonField.COLUMN_NAME) String columnName,
                                @JsonProperty(JsonField.COLUMN_EXPRESSION) String columnExpression) {

        super(type, columnName);
        this.columnExpression = Objects.requireNonNull(columnExpression, JsonField.COLUMN_EXPRESSION);
    }
}
