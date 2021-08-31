package it.luca.aurora.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.extract.Extract;
import it.luca.aurora.configuration.metadata.load.Load;
import it.luca.aurora.configuration.metadata.transform.Transform;
import lombok.Getter;

@Getter
public class EtlConfiguration {

    private final Extract extract;
    private final Transform transform;
    private final Load load;

    @JsonCreator
    public EtlConfiguration(@JsonProperty(JsonField.EXTRACT) Extract extract,
                            @JsonProperty(JsonField.TRANSFORM) Transform transform,
                            @JsonProperty(JsonField.LOAD) Load load) {

        this.extract = extract;
        this.transform = transform;
        this.load = load;
    }
}
