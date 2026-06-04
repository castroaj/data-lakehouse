package com.example.lakehouse.ingestion.kafka;

import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kafka010.KafkaOffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.lakehouse.config.IngestionConfig;

/**
 * A Kafka source that supports both streaming and batch reads based on offset
 * ranges.
 *
 * @author Alexander Castro
 * @see IKafkaSource
 */
public class KafkaSource implements IKafkaSource {

    // Spark DataStreamReader/DataFrameReader format name for the Kafka data source.
    private static final String FORMAT = "kafka";

    // Option keys for the kafka data source
    private static final String OPT_SUBSCRIBE = "subscribe";
    private static final String OPT_ASSIGN = "assign";
    private static final String OPT_STARTING_OFFSETS = "startingOffsets";
    private static final String OPT_ENDING_OFFSETS = "endingOffsets";
    private static final String OPT_MAX_OFFSETS_PER_TRIGGER = "maxOffsetsPerTrigger";
    // Prevents job crash when Kafka log compaction or retention expiry removes offsets
    // the consumer hasn't read yet; such gaps are expected in long-running pipelines.
    private static final String OPT_FAIL_ON_DATA_LOSS = "failOnDataLoss";

    // Spark passes kafka.* options directly to the underlying Kafka consumer.
    // The suffix is taken from the public ConsumerConfig constants in
    // kafka-clients.
    private static final String OPT_BOOTSTRAP_SERVERS = "kafka." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
    private static final String OPT_GROUP_ID = "kafka." + ConsumerConfig.GROUP_ID_CONFIG;

    private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);

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
        checkNotClosed();
        logger.info("Opening streaming read: topic={}, bootstrapServers={}",
                config.topic(), config.bootstrapServers());
        return spark.readStream()
                .format(FORMAT)
                .option(OPT_BOOTSTRAP_SERVERS, config.bootstrapServers())
                .option(OPT_SUBSCRIBE, config.topic())
                .option(OPT_GROUP_ID, config.groupId())
                .option(OPT_STARTING_OFFSETS, config.startingOffsets())
                .option(OPT_MAX_OFFSETS_PER_TRIGGER, config.maxOffsetsPerTrigger())
                .option(OPT_FAIL_ON_DATA_LOSS, false)
                .load();
    }

    @Override
    public Dataset<Row> readBatch(KafkaOffsetRange range) {
        checkNotClosed();
        Objects.requireNonNull(range, "offset range must not be null");
        TopicPartition tp = range.topicPartition();
        return spark.read()
                .format(FORMAT)
                .option(OPT_BOOTSTRAP_SERVERS, config.bootstrapServers())
                .option(OPT_ASSIGN, String.format("{\"%s\":[%d]}", tp.topic(), tp.partition()))
                .option(OPT_STARTING_OFFSETS,
                        String.format("{\"%s\":{\"%d\":%d}}", tp.topic(), tp.partition(), range.fromOffset()))
                .option(OPT_ENDING_OFFSETS,
                        String.format("{\"%s\":{\"%d\":%d}}", tp.topic(), tp.partition(), range.untilOffset()))
                .load();
    }

    @Override
    public void close() {
        if (!closed) {
            logger.info("Closing KafkaSource for topic '{}'", config.topic());
            closed = true;
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException(
                    "KafkaSource for topic '" + config.topic() + "' is closed");
        }
    }
}
