package it.luca.aurora.core.configuration.yaml;

public class UnExistingDataSourceException extends RuntimeException {

    public UnExistingDataSourceException(String id) {

        super(String.format("DataSource %s does not exist", id));
    }
}
