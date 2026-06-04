package com.example.lakehouse.ingestion.kafka;

import com.example.lakehouse.exception.LakehouseException;

/**
 * Exception thrown when the Spark Kafka source cannot be initialized or
 * encounters a fatal error during a read operation.
 *
 * @author Alexander Castro
 * @see KafkaSource
 */
public class KafkaSourceException extends LakehouseException {

    /**
     * Creates a new KafkaSourceException with the given message.
     *
     * @param message the error message describing the exception
     */
    public KafkaSourceException(String message) {
        super(message);
    }

    /**
     * Creates a new KafkaSourceException with the given message and cause.
     *
     * @param message the error message describing the exception
     * @param cause   the underlying cause of the exception
     */
    public KafkaSourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
