package com.example.lakehouse.ingestion.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kafka010.KafkaOffsetRange;

import com.example.lakehouse.config.IngestionConfig;

public class KafkaSource implements IKafkaSource {

    private final SparkSession spark;
    private final KafkaSourceConfig config;
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
