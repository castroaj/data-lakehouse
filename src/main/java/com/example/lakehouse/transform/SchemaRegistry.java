package com.example.lakehouse.transform;

import com.example.lakehouse.transform.schema.SchemaProvider;
import org.apache.spark.sql.types.StructType;

/**
 * Delegates topic-to-schema lookups to an underlying {@link SchemaProvider},
 * providing a stable internal API that decouples consumers from the provider
 * implementation.
 *
 * @author Alexander Castro
 * @see SchemaProvider
 */
public class SchemaRegistry {

    /** The underlying provider that performs the actual schema resolution. */
    private final SchemaProvider provider;

    /**
     * Constructs a SchemaRegistry backed by the given provider.
     *
     * @param provider the schema provider to delegate to; must not be null
     */
    public SchemaRegistry(SchemaProvider provider) {
        this.provider = provider;
    }

    /**
     * Returns the Spark StructType for the given Kafka topic.
     *
     * @param topic the Kafka topic name; must not be blank
     * @return the registered StructType; never null
     * @throws com.example.lakehouse.exception.SchemaNotFoundException if no schema is registered for the topic
     */
    public StructType schemaFor(String topic) {
        return provider.schemaFor(topic);
    }
}
