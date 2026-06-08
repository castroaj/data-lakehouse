package com.example.lakehouse.sink;

import com.example.lakehouse.config.IngestionConfig;
import com.example.lakehouse.transform.TransformResult;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Writes transformed records to Delta Lake and routes malformed rows to a
 * dead-letter path on S3, running both as concurrent Structured Streaming queries.
 *
 * @author Alexander Castro
 * @see Sink
 */
public class DeltaSink implements Sink {

    private static final Logger log = LoggerFactory.getLogger(DeltaSink.class);

    /**
     * Runtime configuration supplying S3 paths, trigger interval, and checkpoint
     * locations for both streaming queries.
     */
    private final IngestionConfig config;

    /**
     * The Spark session shared with the rest of the ingestion job. A single session
     * is reused to avoid the overhead of creating multiple Spark contexts.
     */
    private final SparkSession spark;

    /**
     * The two active streaming queries started by {@link #write(TransformResult)};
     * retained so {@link #close()} can stop them cleanly on shutdown.
     */
    private List<StreamingQuery> activeQueries;

    /**
     * Constructs a DeltaSink and registers the metrics listener on the Spark
     * streaming context so query-level events are captured before any query starts.
     *
     * @param spark           the shared Spark session; must not be null
     * @param config          ingestion config providing S3 paths and trigger settings; must not be null
     * @param metricsListener listener registered on the streaming context for query metrics
     */
    public DeltaSink(SparkSession spark, IngestionConfig config, StreamingQueryListener metricsListener) {
        this.spark = spark;
        this.config = config;
        // Register before any query starts so no lifecycle events are missed
        spark.streams().addListener(metricsListener);
    }

    /**
     * Starts two concurrent Structured Streaming queries: valid rows are written
     * to a Delta Lake table and dead-letter rows are written to a JSON path on S3.
     * Both queries share the same processing interval to keep the streams temporally
     * aligned.
     *
     * @param result the split result from JsonTransformer; must not be null
     * @return an unmodifiable list of the two running queries, in order: [valid, dead-letter]
     */
    @Override
    public List<StreamingQuery> write(TransformResult result) {
        String bucket = "s3a://" + config.s3BucketName() + "/";
        int triggerSeconds = config.triggerIntervalSeconds();

        StreamingQuery validQuery = result.valid().writeStream()
                .queryName("lakehouse-valid-sink")
                .format("delta")
                .option("checkpointLocation", bucket + config.checkpointPath() + "/valid")
                .trigger(Trigger.ProcessingTime(triggerSeconds + " seconds"))
                .start(bucket + config.deltaTablePath());

        StreamingQuery deadLetterQuery = result.deadLetter().writeStream()
                .queryName("lakehouse-dead-letter-sink")
                // JSON rather than Delta so dead-letter rows are human-readable and
                // recoverable without a Delta reader — tooling access matters more than
                // ACID guarantees for a diagnostic path.
                .format("json")
                .option("checkpointLocation", bucket + config.checkpointPath() + "/dead-letter")
                .trigger(Trigger.ProcessingTime(triggerSeconds + " seconds"))
                .start(bucket + config.deadLetterPath());

        log.info("streams_started valid_path={} dead_letter_path={}", config.deltaTablePath(), config.deadLetterPath());
        activeQueries = List.of(validQuery, deadLetterQuery);
        return activeQueries;
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
