package com.example.lakehouse.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Transforms a raw Kafka streaming {@link Dataset} into a {@link TransformResult}
 * containing valid and dead-letter partitions based on schema conformance.
 *
 * @author Alexander Castro
 */
public interface Transformer extends AutoCloseable {

    /**
     * Applies the transformation to the raw input stream, splitting rows into
     * valid and dead-letter partitions.
     *
     * @param input the raw Kafka streaming dataset; must not be null
     * @return a TransformResult with valid and dead-letter datasets; never null
     */
    TransformResult transform(Dataset<Row> input);

    /**
     * Releases any resources held by this transformer. Idempotent.
     */
    @Override
    default void close() {}
}
