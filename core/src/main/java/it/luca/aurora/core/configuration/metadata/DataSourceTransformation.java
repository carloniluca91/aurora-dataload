package it.luca.aurora.core.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class DataSourceTransformation {

    private final String expression;
    private final String alias;

    @JsonCreator
    public DataSourceTransformation(@JsonProperty("expression") String expression,
                                    @JsonProperty("expression") String alias) {

        this.expression = expression;
        this.alias = alias;
    }
}
