package com.example.lakehouse.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark {@link StreamingQueryListener} that forwards streaming query metrics to
 * a Micrometer {@link MeterRegistry} and logs key lifecycle events.
 *
 * @author Alexander Castro
 */
public class LakehouseMetricsListener extends StreamingQueryListener {

    private static final Logger log = LoggerFactory.getLogger(LakehouseMetricsListener.class);

    // Query names used to route per-sink metrics; must match the names assigned in DeltaSink.
    static final String VALID_SINK_NAME = "lakehouse-valid-sink";
    static final String DEAD_LETTER_SINK_NAME = "lakehouse-dead-letter-sink";

    /** Counts total valid records written to Delta Lake. */
    private final Counter validRecordsCounter;

    /** Counts total records routed to the dead-letter path. */
    private final Counter deadLetterCounter;

    /** Counts total micro-batches committed across all sinks. */
    private final Counter batchCounter;

    /** Records the end-to-end processing duration for each micro-batch in milliseconds. */
    private final DistributionSummary batchDuration;

    /**
     * Constructs a LakehouseMetricsListener and registers all meters with the
     * provided registry.
     *
     * @param registry the meter registry to register metrics with; must not be null
     */
    public LakehouseMetricsListener(MeterRegistry registry) {
        this.validRecordsCounter = registry.counter("lakehouse.records.valid");
        this.deadLetterCounter = registry.counter("lakehouse.records.dead_letter");
        this.batchCounter = registry.counter("lakehouse.batches.total");
        this.batchDuration = DistributionSummary.builder("lakehouse.batch.duration.ms")
                .baseUnit("milliseconds")
                .register(registry);
    }

    /**
     * Logs a stream-started event at info level.
     */
    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        log.info("stream_started query_id={} name={}", event.id(), event.name());
    }

    /**
     * Increments per-sink counters and records batch duration on each micro-batch
     * commit. Routing is done by query name so each sink's metrics remain separate.
     */
    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        var progress = event.progress();
        String name = progress.name();
        long inputRows = progress.numInputRows();
        long durationMs = progress.batchDuration();

        if (VALID_SINK_NAME.equals(name)) {
            validRecordsCounter.increment(inputRows);
            batchCounter.increment();
            batchDuration.record(durationMs);
            log.info("batch_committed sink=valid rows={} duration_ms={}", inputRows, durationMs);
        } else if (DEAD_LETTER_SINK_NAME.equals(name)) {
            deadLetterCounter.increment(inputRows);
            log.info("batch_committed sink=dead_letter rows={}", inputRows);
        }
    }

    /**
     * Logs stream termination at error level if the query ended with an exception,
     * or at info level for a clean shutdown.
     */
    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        if (event.exception().isDefined()) {
            log.error("stream_terminated query_id={} error={}", event.id(), event.exception().get());
        } else {
            log.info("stream_terminated query_id={}", event.id());
        }
    }
}
