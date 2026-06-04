package com.example.lakehouse.exception;

/**
 * Exception thrown when configuration validation fails, e.g. due to missing or
 * invalid environment variables
 */
public class ConfigurationException extends LakehouseException {

    /**
     * Creates a new ConfigurationException with the given message
     */
    public ConfigurationException(String message) {
        super(message);
    }
}
