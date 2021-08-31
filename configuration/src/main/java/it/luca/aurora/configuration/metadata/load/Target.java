package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

@Getter
public class Target {

    private final String trusted;
    private final String error;

    @JsonCreator
    public Target(@JsonProperty(JsonField.TRUSTED) String trusted,
                  @JsonProperty(JsonField.ERROR) String error) {

        this.trusted = trusted;
        this.error = error;
    }
}
