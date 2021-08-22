package it.luca.aurora.core.configuration.yaml;

public class UnExistingPropertyException extends RuntimeException {

    public UnExistingPropertyException(String key) {

        super(String.format("Key %s does not exist", key));
    }
}
