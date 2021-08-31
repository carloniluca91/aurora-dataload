package it.luca.aurora.configuration.yaml;

public class UnExistingDataSourceException extends Exception {

    public UnExistingDataSourceException(String id) {

        super(String.format("DataSource %s does not exist", id));
    }
}
