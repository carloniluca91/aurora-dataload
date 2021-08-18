package it.luca.aurora.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class DataSource {

    private final String id;
    private final String configurationFile;

    @JsonCreator
    public DataSource(@JsonProperty("id") String id,
                      @JsonProperty("configurationFile") String configurationFile) {

        this.id = id;
        this.configurationFile = configurationFile;
    }
}
