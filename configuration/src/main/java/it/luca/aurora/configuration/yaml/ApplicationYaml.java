package it.luca.aurora.configuration.yaml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    /**
     * Performs property interpolation
     * @return copy of this instance of {@link ApplicationYaml} with interpolated properties
     * @throws UnExistingPropertyException if interpolation fails
     */

    public ApplicationYaml withInterpolation() throws UnExistingPropertyException {

        Pattern pattern = Pattern.compile("(\\$\\{([\\w.]+)})");
        List<String> keySet = new ArrayList<>(properties.keySet());
        Map<String, String> interpolatedProperties = new HashMap<>();

        for (String key: keySet) {

            String value = properties.get(key);
            Matcher matcher = pattern.matcher(value);
            String interpolatedValue = value;
            while (matcher.find()) {
                interpolatedValue = interpolatedValue.replace(matcher.group(1), this.getProperty(matcher.group(2)));
            }

            interpolatedProperties.put(key, interpolatedValue);
        }

        return new ApplicationYaml(interpolatedProperties, dataSources);
    }

    /**
     * Returns value of provided property if it exists
     * @param key property
     * @return value of property
     * @throws UnExistingPropertyException if provided property does not exist
     */

    public String getProperty(String key) throws UnExistingPropertyException {

        if (properties.containsKey(key)) {
            return properties.get(key);
        } else throw new UnExistingPropertyException(key);
    }

    /**
     * Return instance of {@link DataSource} with given id if it exists
     * @param id dataSource id
     * @return {@link DataSource}
     * @throws UnExistingDataSourceException if no dataSource is found
     * @throws DuplicatedDataSourceException if more than one dataSource has such id
     */

    public DataSource getDataSourceWithId(String id) throws UnExistingDataSourceException, DuplicatedDataSourceException {

        List<DataSource> matchingDataSources = dataSources.stream()
                .filter(x -> x.getId().equalsIgnoreCase(id))
                .collect(Collectors.toList());

        int numberOfMatches = matchingDataSources.size();
        if (numberOfMatches == 0) {
            throw new UnExistingDataSourceException(id);
        } else if (numberOfMatches > 1) {
            throw new DuplicatedDataSourceException(id, matchingDataSources);
        } else {
            return matchingDataSources.get(0);
        }
    }
}
