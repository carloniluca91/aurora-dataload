package it.luca.aurora.configuration.datasource;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.configuration2.PropertiesConfiguration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AllArgsConstructor
public class DataSource {

    public static final String TOKEN_REPLACE_REGEX = "\\$\\{([\\w.]+)}";

    @Getter
    private final String id;

    @Getter
    private final String metadataFilePath;

    @Override
    public String toString() {

        return String.format("Id: %s, Metadata: %s", id, metadataFilePath);
    }

    /**
     * Performs interolation on metadata file path using given {@link PropertiesConfiguration}, if necessary
     * @param properties instance of {@link PropertiesConfiguration}
     * @return input string with interpolation if necessary, input itself otherwise
     */

    public DataSource withInterpolation(PropertiesConfiguration properties) {

        Matcher matcher = Pattern.compile(TOKEN_REPLACE_REGEX).matcher(metadataFilePath);
        return matcher.find() ?
                new DataSource(id, matcher.replaceAll(properties.getString(matcher.group(1)))) :
                this;
    }
}
