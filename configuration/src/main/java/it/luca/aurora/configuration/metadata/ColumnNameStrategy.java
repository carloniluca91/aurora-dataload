package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class ColumnNameStrategy extends PartitionStrategy {

    public ColumnNameStrategy(@JsonProperty("id") String id,
                              @JsonProperty("columnName") String columnName) {
        super(id, columnName);
    }
}
