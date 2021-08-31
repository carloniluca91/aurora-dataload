package it.luca.aurora.configuration.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

import java.util.List;

@Getter
public class Transform {

    private final List<String> filters;
    private final List<String> transformations;

    @JsonCreator
    public Transform(@JsonProperty(JsonField.FILTERS) List<String> filters,
                     @JsonProperty(JsonField.TRANSFORMATIONS) List<String> transformations) {

        this.filters = filters;
        this.transformations = transformations;
    }
}
