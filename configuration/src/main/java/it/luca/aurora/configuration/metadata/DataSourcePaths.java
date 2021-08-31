package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class DataSourcePaths {

    private final String landing;
    private final String error;
    private final String success;

    @JsonCreator
    public DataSourcePaths(@JsonProperty(JsonField.LANDING) String landing,
                           @JsonProperty(JsonField.ERROR) String error,
                           @JsonProperty(JsonField.SUCCESS) String success) {

        this.landing = landing;
        this.error = error;
        this.success = success;
    }
}
