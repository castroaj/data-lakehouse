package com.example.lakehouse.sink;

import com.example.lakehouse.config.DeltaSinkConfig;
import com.example.lakehouse.config.IngestionConfig;
import com.example.lakehouse.transform.TransformResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Writes transformed records to Delta Lake and routes malformed rows to a
 * dead-letter path on S3, running both as concurrent Structured Streaming queries.
 *
 * <p>Delta table tuning (partition columns, file sizing, auto-optimize) is
 * controlled via a {@link DeltaSinkConfig} supplied at construction time.
 * Only the valid-records query benefits from these options; the dead-letter
 * JSON path is written with plain Spark defaults for easy human inspection.
 *
 * @author Alexander Castro
 * @see Sink
 */
public class DeltaSink implements Sink {

    private static final Logger log = LoggerFactory.getLogger(DeltaSink.class);

    private final SparkSession spark;
    private final String deltaTablePath;
    private final String checkpointBasePath;
    private final String deadLetterPath;
    private final int triggerIntervalSeconds;
    private final DeltaSinkConfig deltaSinkConfig;

    /**
     * The two active streaming queries started by {@link #write(TransformResult)};
     * retained so {@link #close()} can stop them cleanly on shutdown.
     */
    private List<StreamingQuery> activeQueries;

    /**
     * Constructs a DeltaSink from fully-resolved absolute paths and registers the
     * metrics listener on the streaming context before any query starts.
     *
     * <p>This constructor is package-private so that unit tests can supply local
     * filesystem paths (e.g. a JUnit {@code @TempDir}) instead of {@code s3a://}
     * URIs, without requiring AWS credentials or S3Mock infrastructure.
     *
     * @param spark                 shared Spark session; must not be null
     * @param deltaTablePath        absolute path where valid records are written
     * @param checkpointBasePath    parent path for checkpoint sub-directories
     *                              ({@code /valid} and {@code /dead-letter} are appended)
     * @param deadLetterPath        absolute path where malformed records are written
     * @param triggerIntervalSeconds micro-batch interval in seconds; must be ≥ 1
     * @param deltaSinkConfig       Delta table tuning options; must not be null
     * @param metricsListener       listener registered before any query starts
     */
    DeltaSink(SparkSession spark,
              String deltaTablePath,
              String checkpointBasePath,
              String deadLetterPath,
              int triggerIntervalSeconds,
              DeltaSinkConfig deltaSinkConfig,
              StreamingQueryListener metricsListener) {
        this.spark = spark;
        this.deltaTablePath = deltaTablePath;
        this.checkpointBasePath = checkpointBasePath;
        this.deadLetterPath = deadLetterPath;
        this.triggerIntervalSeconds = triggerIntervalSeconds;
        this.deltaSinkConfig = deltaSinkConfig;
        // Register before any query starts so no lifecycle events are missed
        spark.streams().addListener(metricsListener);
    }

    /**
     * Constructs a DeltaSink from the application {@link IngestionConfig}, deriving
     * all S3 paths from the config's bucket name and path segments, and registers
     * the metrics listener on the streaming context.
     *
     * @param spark           the shared Spark session; must not be null
     * @param config          ingestion config providing S3 paths and trigger settings; must not be null
     * @param metricsListener listener registered on the streaming context for query metrics
     */
    public DeltaSink(SparkSession spark, IngestionConfig config, StreamingQueryListener metricsListener) {
        this(spark,
                "s3a://" + config.s3BucketName() + "/" + config.deltaTablePath(),
                "s3a://" + config.s3BucketName() + "/" + config.checkpointPath(),
                "s3a://" + config.s3BucketName() + "/" + config.deadLetterPath(),
                config.triggerIntervalSeconds(),
                config.deltaSinkConfig(),
                metricsListener);
    }

    /**
     * Starts two concurrent Structured Streaming queries: valid rows are written
     * to a Delta Lake table and dead-letter rows are written to a JSON path.
     * Both queries share the same processing interval to keep the streams temporally
     * aligned.
     *
     * <p>Delta tuning options from {@link DeltaSinkConfig} are applied to the
     * valid-records writer only:
     * <ul>
     *   <li>Partition columns via {@code partitionBy()} for query-engine file pruning</li>
     *   <li>Target file size to prevent small-file accumulation</li>
     *   <li>Auto-optimized writes to right-size Parquet files per micro-batch</li>
     *   <li>Auto-compaction to merge residual small files after each batch</li>
     *   <li>Schema merge for backward-compatible schema evolution</li>
     *   <li>Max records per file to cap row-group size</li>
     * </ul>
     *
     * @param result the split result from JsonTransformer; must not be null
     * @return an unmodifiable list of the two running queries, in order: [valid, dead-letter]
     */
    @Override
    public List<StreamingQuery> write(TransformResult result) {
        Trigger trigger = Trigger.ProcessingTime(triggerIntervalSeconds + " seconds");

        StreamingQuery validQuery = applyDeltaOptions(
                result.valid().writeStream()
                        .queryName("lakehouse-valid-sink")
                        .format("delta")
                        .outputMode("append")
                        .option("checkpointLocation", checkpointBasePath + "/valid")
                        .trigger(trigger))
                .start(deltaTablePath);

        StreamingQuery deadLetterQuery = result.deadLetter().writeStream()
                .queryName("lakehouse-dead-letter-sink")
                // JSON rather than Delta so dead-letter rows are human-readable and
                // recoverable without a Delta reader — tooling access matters more than
                // ACID guarantees for a diagnostic path.
                .format("json")
                .outputMode("append")
                .option("checkpointLocation", checkpointBasePath + "/dead-letter")
                .trigger(trigger)
                .start(deadLetterPath);

        log.info("streams_started valid_path={} dead_letter_path={}", deltaTablePath, deadLetterPath);
        activeQueries = List.of(validQuery, deadLetterQuery);
        return activeQueries;
    }

    /**
     * Applies {@link DeltaSinkConfig} tuning options to the provided writer and
     * returns it. Only non-null / non-empty values produce an {@code option()} call
     * so that absent settings inherit the Delta or Spark runtime defaults.
     */
    private DataStreamWriter<Row> applyDeltaOptions(DataStreamWriter<Row> writer) {
        if (!deltaSinkConfig.partitionColumns().isEmpty()) {
            writer = writer.partitionBy(deltaSinkConfig.partitionColumns().toArray(new String[0]));
        }
        if (deltaSinkConfig.targetFileSizeBytes() != null) {
            writer = writer.option("delta.targetFileSize", deltaSinkConfig.targetFileSizeBytes().toString());
        }
        if (deltaSinkConfig.optimizeWrite()) {
            writer = writer.option("optimizeWrite", "true");
        }
        if (deltaSinkConfig.autoCompact()) {
            writer = writer.option("autoCompact", "true");
        }
        if (deltaSinkConfig.mergeSchema()) {
            writer = writer.option("mergeSchema", "true");
        }
        if (deltaSinkConfig.maxRecordsPerFile() != null) {
            writer = writer.option("maxRecordsPerFile", deltaSinkConfig.maxRecordsPerFile().toString());
        }
        return writer;
    }

    /**
     * Stops all active streaming queries. Safe to call before
     * {@link #write(TransformResult)} or multiple times. A timeout on an individual
     * query stop is logged as a warning rather than rethrown so the remaining queries
     * still get a chance to stop cleanly.
     */
    @Override
    public void close() {
        if (activeQueries != null) {
            for (StreamingQuery q : activeQueries) {
                try {
                    q.stop();
                } catch (TimeoutException e) {
                    log.warn("timeout stopping query name={}", q.name());
                }
            }
        }
    }
}
