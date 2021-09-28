package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

import java.time.format.DateTimeFormatter;

import static java.util.Objects.requireNonNull;

@Getter
public class FileNameRegexConfiguration {

    private final Integer regexGroup;
    private final String inputPattern;
    private final String outputPattern;

    @JsonCreator
    public FileNameRegexConfiguration(@JsonProperty(JsonField.REGEX_GROUP) Integer regexGroup,
                                      @JsonProperty(JsonField.INPUT_PATTERN) String inputPattern,
                                      @JsonProperty(JsonField.OUTPUT_PATTERN) String outputPattern) {

        this.regexGroup = requireNonNull(regexGroup, JsonField.REGEX_GROUP);
        this.inputPattern = requireNonNull(inputPattern, JsonField.INPUT_PATTERN);
        this.outputPattern = requireNonNull(outputPattern, JsonField.OUTPUT_PATTERN);
    }

    /**
     * Return formatter based on inputPattern
     * @return instance of {@link DateTimeFormatter}
     */

    public DateTimeFormatter getInputFormatter() {

        return DateTimeFormatter.ofPattern(inputPattern);
    }

    /**
     * Return formatter based on outputPattern
     * @return instance of {@link DateTimeFormatter}
     */

    public DateTimeFormatter getOutputFormatter() {

        return DateTimeFormatter.ofPattern(outputPattern);
    }
}
