package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.Optional;

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

    public boolean isAliasPresent() {

        return Optional.ofNullable(alias).isPresent();
    }
}
