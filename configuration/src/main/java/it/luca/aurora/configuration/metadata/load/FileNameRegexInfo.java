package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

@Slf4j
@Getter
public class FileNameRegexInfo extends PartitionInfo {

    private final FileNameRegexConfiguration configuration;

    @JsonCreator
    public FileNameRegexInfo(@JsonProperty(JsonField.TYPE) String type,
                             @JsonProperty(JsonField.COLUMN_NAME) String columnName,
                             @JsonProperty(JsonField.CONFIGURATION) FileNameRegexConfiguration configuration) {

        super(type, columnName);
        this.configuration = requireNonNull(configuration, JsonField.CONFIGURATION);
    }

    /**
     * Extract the date from the name of file at given path
     * @param regex regex to use for extracting embedded date
     * @param filePath {@link Path} of file
     * @return date from file name
     * @throws FileNameRegexException if regex does not match file name
     */

    public String getDateFromFileName(String regex, Path filePath) throws FileNameRegexException {

        String fileName = filePath.getName();
        Matcher matcher = Pattern.compile(regex).matcher(fileName);
        if (matcher.matches()) {
            String partitionValue = LocalDate
                    .parse(matcher.group(configuration.getRegexGroup()), configuration.getInputFormatter())
                    .format(configuration.getOutputFormatter());
            log.info("Partitioning value for column {} will be {}", columnName, partitionValue);
            return partitionValue;
        } else throw new FileNameRegexException(regex, filePath);
    }
}
