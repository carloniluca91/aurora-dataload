package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

@Getter
public class FileNameRegexConfiguration {

    private final Integer regexGroup;
    private final String inputPattern;
    private final String outputPattern;

    @JsonCreator
    public FileNameRegexConfiguration(@JsonProperty(JsonField.REGEX_GROUP) Integer regexGroup,
                                      @JsonProperty(JsonField.INPUT_PATTERN) String inputPattern,
                                      @JsonProperty(JsonField.OUTPUT_PATTERN) String outputPattern) {

        this.regexGroup = regexGroup;
        this.inputPattern = inputPattern;
        this.outputPattern = outputPattern;
    }
}
