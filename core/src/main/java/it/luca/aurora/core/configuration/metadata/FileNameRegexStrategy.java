package it.luca.aurora.core.configuration.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.apache.hadoop.fs.Path;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
public class FileNameRegexStrategy extends PartitionStrategy {

    private final Integer regexGroup;
    private final String inputPattern;
    private final String outputPattern;

    @JsonCreator
    public FileNameRegexStrategy(@JsonProperty("id") String id,
                                 @JsonProperty("columnName") String columnName,
                                 @JsonProperty("regexGroup") Integer regexGroup,
                                 @JsonProperty("inputPattern") String inputPattern,
                                 @JsonProperty("outputPattern") String outputPattern) {

        super(id, columnName);
        this.regexGroup = regexGroup;
        this.inputPattern = inputPattern;
        this.outputPattern = outputPattern;
    }

    public String getDateFromFileName(String regex, Path filePath) throws Exception {

        String fileName = filePath.getName();
        Matcher matcher = Pattern.compile(regex).matcher(fileName);
        if (matcher.matches()) {
            String dateFromFileName = matcher.group(regexGroup);
            return LocalDate.parse(dateFromFileName, DateTimeFormatter.ofPattern(inputPattern))
                    .format(DateTimeFormatter.ofPattern(outputPattern));
        } else throw new FileNameRegexStrategyException(regex, filePath);
    }
}
