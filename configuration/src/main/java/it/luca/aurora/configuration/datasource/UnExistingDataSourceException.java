package it.luca.aurora.configuration.datasource;

public class UnExistingDataSourceException extends Exception {

    public UnExistingDataSourceException(String id) {

        super(String.format("DataSource %s does not exist", id));
    }
}
