package com.example.lakehouse.ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Interface representing a generic data source for the lakehouse ingestion
 * pipeline.
 * 
 * @author Alexander Castro
 */
public interface Source extends AutoCloseable {

    /**
     * Returns a streaming {@link Dataset} reading from the provided source.
     *
     * <p>
     * The dataset is not materialized until a streaming query is started against
     * it.
     * Call this method at most once per source instance.
     *
     * @return an unbounded streaming dataset with the source schema
     * @throws IllegalStateException if this source has already been closed
     * @throws KafkaSourceException  if the Spark Kafka source cannot be initialized
     */
    Dataset<Row> readStream();

}
