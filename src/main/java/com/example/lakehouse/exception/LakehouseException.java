package com.example.lakehouse.exception;

public abstract class LakehouseException extends RuntimeException {

    protected LakehouseException(String message) {
        super(message);
    }

    protected LakehouseException(String message, Throwable cause) {
        super(message, cause);
    }
}
