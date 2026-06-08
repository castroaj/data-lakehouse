package com.example.lakehouse.transform.schema;

import com.example.lakehouse.exception.SchemaNotFoundException;
import org.apache.spark.sql.types.StructType;

/**
 * Resolves Spark {@link StructType} schemas by Kafka topic name.
 *
 * @author Alexander Castro
 */
public interface SchemaProvider {

    /**
     * Returns the Spark {@link StructType} for the given Kafka topic.
     *
     * @param topic the Kafka topic name; must not be blank
     * @return the registered StructType; never null
     * @throws SchemaNotFoundException if no schema is registered for the topic
     */
    StructType schemaFor(String topic);
}
