package it.luca.aurora.configuration.yaml;

public class UnExistingPropertyException extends Exception {

    public UnExistingPropertyException(String key) {

        super(String.format("Key %s does not exist", key));
    }
}
