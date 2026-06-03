package com.example.lakehouse.ingestion.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.kafka010.KafkaOffsetRange;

import com.example.lakehouse.ingestion.Source;

/**
 * Interface for a Kafka source that supports both streaming and batch reads
 * based on offset ranges.
 * 
 * @author Alexander Castro
 */
public interface IKafkaSource extends Source {

    /**
     * Returns a bounded {@link Dataset} reading records within the given offset
     * range.
     *
     * <p>
     * Useful for backfill and replay scenarios where a specific window of messages
     * must be reprocessed.
     *
     * @param range the topic, start offsets, and end offsets to read; must not be
     *              null
     * @return a batch dataset bounded by the supplied offset range
     * @throws IllegalStateException if this source has already been closed
     * @throws KafkaSourceException  if the Spark Kafka source cannot be initialized
     */
    Dataset<Row> readBatch(KafkaOffsetRange range);
}
