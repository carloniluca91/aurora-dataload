package it.luca.aurora.core.configuration.yaml;

public class UnExistingPropertyException extends Exception {

    public UnExistingPropertyException(String key) {

        super(String.format("Key %s does not exist", key));
    }
}
