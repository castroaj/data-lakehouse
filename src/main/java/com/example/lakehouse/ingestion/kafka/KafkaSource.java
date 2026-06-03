package com.example.lakehouse.ingestion.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kafka010.KafkaOffsetRange;

import com.example.lakehouse.config.IngestionConfig;

/**
 * A Kafka source that supports both streaming and batch reads based on offset
 * ranges.
 *
 * @author Alexander Castro
 * @see IKafkaSource
 */
public class KafkaSource implements IKafkaSource {

    /**
     * The Spark session to use for reading from Kafka. This should be a long-lived
     * session that is shared across the ingestion job to avoid unnecessary overhead
     * of creating multiple sessions.
     */
    private final SparkSession spark;

    /**
     * The configuration for this Kafka source, containing topic, bootstrap servers,
     * consumer group ID, and other settings.
     */
    private final KafkaSourceConfig config;

    /**
     * A flag indicating whether this source has been closed.
     */
    private volatile boolean closed = false;

    /**
     * Constructs a KafkaSource with the given Spark session and ingestion config
     * 
     * @param spark  the Spark session to use for reading from Kafka; must not be
     *               null
     * @param config the ingestion configuration containing Kafka source settings;
     */
    public KafkaSource(SparkSession spark, IngestionConfig config) {
        this(spark, config.kafkaSourceConfig());
    }

    /**
     * Constructs a KafkaSource with the given Spark session
     * and Kafka source config
     * 
     * @param spark  the Spark session to use for reading from Kafka; must not be
     *               null
     * @param config the Kafka source configuration containing topic, bootstrap
     *               servers, and other settings
     */
    public KafkaSource(SparkSession spark, KafkaSourceConfig config) {
        this.spark = spark;
        this.config = config;
    }

    @Override
    public Dataset<Row> readStream() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Dataset<Row> readBatch(KafkaOffsetRange range) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("not implemented");
    }
}
