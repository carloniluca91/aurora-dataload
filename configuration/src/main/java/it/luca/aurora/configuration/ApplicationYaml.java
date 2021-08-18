package it.luca.aurora.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
public class ApplicationYaml {

    private final Map<String, String> properties;
    private final List<DataSource> dataSources;

    @JsonCreator
    public ApplicationYaml(@JsonProperty("properties") Map<String, String> properties,
                           @JsonProperty("dataSources") List<DataSource> dataSources) {

        this.properties = properties;
        this.dataSources = dataSources;
    }

    public ApplicationYaml withInterpolation() {

        Pattern pattern = Pattern.compile("\\$\\{([\\w|.]+)}");
        properties.forEach((key, value) -> {

            Matcher matcher = pattern.matcher(value);
            if (matcher.matches()) {

                String interpolatedValue = matcher.replaceAll(properties.get(matcher.group(1)));
                properties.put(key, interpolatedValue);
            }
        });

        return this;
    }
}
