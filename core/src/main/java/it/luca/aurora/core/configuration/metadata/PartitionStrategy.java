package it.luca.aurora.core.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "id")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RegexFileNameStrategy.class, name = "FILE_NAME_REGEX")
})
@AllArgsConstructor
public abstract class PartitionStrategy {

    protected final String id;
}
