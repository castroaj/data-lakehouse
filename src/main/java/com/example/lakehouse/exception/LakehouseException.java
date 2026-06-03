package com.example.lakehouse.exception;

/**
 * Base class for all exceptions in the lakehouse application
 * 
 * @author Alexander Castro
 */
public abstract class LakehouseException extends RuntimeException {

    /**
     * Creates a new LakehouseException with the given message
     * 
     * @param message the error message describing the exception
     */
    protected LakehouseException(String message) {
        super(message);
    }

    /**
     * Creates a new LakehouseException with the given message and cause
     * 
     * @param message the error message describing the exception
     * @param cause   the underlying cause of the exception
     */
    protected LakehouseException(String message, Throwable cause) {
        super(message, cause);
    }
}
