package it.luca.aurora.core.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class RegexFileNameStrategy extends PartitionStrategy {

    private final Integer group;
    private final String pattern;

    @JsonCreator
    public RegexFileNameStrategy(@JsonProperty("id") String id,
                                 @JsonProperty("group") Integer group,
                                 @JsonProperty("pattern") String pattern) {

        super(id);
        this.group = group;
        this.pattern = pattern;
    }
}
