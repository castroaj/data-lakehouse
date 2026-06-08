package com.example.lakehouse.exception;

/**
 * Thrown when no Avro schema is registered for a given Kafka topic, either
 * because the schema file is missing from the classpath or the topic name is
 * unknown.
 *
 * @author Alexander Castro
 */
public class SchemaNotFoundException extends LakehouseException {

    /**
     * Creates a new SchemaNotFoundException for the given topic.
     *
     * @param topic the Kafka topic for which no schema was found
     */
    public SchemaNotFoundException(String topic) {
        super("No schema registered for topic: " + topic);
    }
}
