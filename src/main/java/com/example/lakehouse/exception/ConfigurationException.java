package com.example.lakehouse.exception;

/**
 * Thrown when configuration validation fails, for example due to missing or
 * invalid environment variables.
 *
 * @author Alexander Castro
 */
public class ConfigurationException extends LakehouseException {

    /**
     * Creates a new ConfigurationException with the given message.
     *
     * @param message a description of the validation failure; should name the
     *                offending field and explain the constraint
     */
    public ConfigurationException(String message) {
        super(message);
    }
}
