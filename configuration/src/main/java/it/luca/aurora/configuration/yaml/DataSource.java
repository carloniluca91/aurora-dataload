package it.luca.aurora.configuration.yaml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class DataSource {

    private final String id;
    private final String metadataFilePath;

    @Override
    public String toString() {

        return String.format("Id: %s, Metadata: %s", id, metadataFilePath);
    }

    @JsonCreator
    public DataSource(@JsonProperty("id") String id,
                      @JsonProperty("metadataFilePath") String metadataFilePath) {

        this.id = id;
        this.metadataFilePath = metadataFilePath;
    }
}
