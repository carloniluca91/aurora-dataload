package it.luca.aurora.core.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        property = "id",
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = FileNameRegexStrategy.class, name = "FILE_NAME_REGEX"),
        @JsonSubTypes.Type(value = ColumnNameStrategy.class, name = "COLUMN_NAME")
})
public abstract class PartitionStrategy {

    protected final String id;
    protected final String columnName;
}
