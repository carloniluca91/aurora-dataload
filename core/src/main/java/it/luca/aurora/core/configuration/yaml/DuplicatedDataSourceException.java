package it.luca.aurora.core.configuration.yaml;

import java.util.List;
import java.util.stream.Collectors;

public class DuplicatedDataSourceException extends Exception {

    public DuplicatedDataSourceException(String id, List<DataSource> dataSources) {

        super(String.format("Found %s %s with id %s (%s)",
                dataSources.size(),
                DataSource.class.getSimpleName(),
                id,
                dataSources.stream().map(DataSource::toString).collect(Collectors.joining("|"))));
    }
}
