package com.example.lakehouse.ingestion.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.kafka010.KafkaOffsetRange;

public interface IKafkaSource {

    /**
     * Returns a streaming {@link Dataset} subscribed to the configured Kafka topic.
     *
     * <p>
     * The dataset is not materialized until a streaming query is started against
     * it.
     * Call this method at most once per source instance.
     *
     * @return an unbounded streaming dataset with the Kafka source schema
     * @throws IllegalStateException if this source has already been closed
     * @throws KafkaSourceException  if the Spark Kafka source cannot be initialized
     */
    Dataset<Row> readStream();

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

    /**
     * Releases any resources held by this source.
     *
     * <p>
     * After calling {@code close()}, all other methods throw
     * {@link IllegalStateException}.
     * Calling {@code close()} more than once has no effect.
     */
    void close();

}
