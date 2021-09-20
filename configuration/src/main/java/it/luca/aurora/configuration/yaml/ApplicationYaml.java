package it.luca.aurora.configuration.yaml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
public class ApplicationYaml {

    public static final String DATASOURCES = "dataSources";

    private final List<DataSource> dataSources;

    @JsonCreator
    public ApplicationYaml(@JsonProperty(DATASOURCES) List<DataSource> dataSources) {

        this.dataSources = Objects.requireNonNull(dataSources, DATASOURCES);
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
