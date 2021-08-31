package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        property = JsonField.TYPE,
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = FileNameRegexInfo.class, name = PartitionInfoType.FILE_NAME_REGEX),
        @JsonSubTypes.Type(value = ColumnExpressionInfo.class, name = PartitionInfoType.COLUMN_EXPRESSION)
})
public abstract class PartitionInfo {

    protected final String type;
    protected final String columnName;

}
