package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Getter
public class FileNameRegexInfo extends PartitionInfo {

    private final FileNameRegexConfiguration configuration;

    @JsonCreator
    public FileNameRegexInfo(@JsonProperty(JsonField.TYPE) String type,
                             @JsonProperty(JsonField.COLUMN_NAME) String columnName,
                             @JsonProperty(JsonField.CONFIGURATION) FileNameRegexConfiguration configuration) {

        super(type, columnName);
        this.configuration = configuration;
    }

    public String getDateFromFileName(String regex, Path filePath) throws Exception {

        String fileName = filePath.getName();
        Matcher matcher = Pattern.compile(regex).matcher(fileName);
        if (matcher.matches()) {
            String partitionValue = LocalDate.parse(matcher.group(configuration.getRegexGroup()),
                            DateTimeFormatter.ofPattern(configuration.getInputPattern()))
                    .format(DateTimeFormatter.ofPattern(configuration.getOutputPattern()));
            log.info("Partitioning value for column {}: {}", columnName, partitionValue);
            return partitionValue;
        } else throw new FileNameRegexException(regex, filePath);
    }
}
