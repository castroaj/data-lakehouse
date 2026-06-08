package com.example.lakehouse.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;

/**
 * Tuning parameters for the Delta Lake streaming sink. All fields are optional
 * (nullable numeric types, empty lists) — absent values let Delta or Spark apply
 * their own defaults.
 *
 * <p>Use {@link #defaults()} for a production-safe baseline: optimized writes
 * enabled, no auto-compaction, no schema evolution.
 *
 * @author Alexander Castro
 */
public record DeltaSinkConfig(

        /**
         * Columns used to partition the Delta table on write. An empty list disables
         * partitioning. Partition keys must exist in the DataFrame schema written by
         * {@code JsonTransformer}; Spark rejects unknown column names at query start.
         *
         * <p>Prefer low-cardinality columns (e.g. {@code eventType}, date-part derived
         * from {@code _ingested_at}) so query engines can prune entire partitions rather
         * than scanning all files.
         */
        @NotNull(message = "DELTA_PARTITION_COLUMNS list must not be null — use an empty list for no partitioning")
        List<String> partitionColumns,

        /**
         * Target size in bytes for each Parquet file produced by Delta's
         * {@code optimizeWrite}. When {@code null}, Delta uses its internal default
         * (~128 MB). Setting this lower (e.g. 64 MB) produces more, smaller files;
         * setting it higher trades write parallelism for fewer files to scan.
         *
         * <p>Has no effect when {@code optimizeWrite} is {@code false}.
         */
        @Min(value = 1, message = "DELTA_TARGET_FILE_SIZE_BYTES must be at least 1 byte")
        Long targetFileSizeBytes,

        /**
         * When {@code true}, Delta rewrites shuffle output to produce right-sized Parquet
         * files without a separate OPTIMIZE run. Recommended for streaming sinks where
         * micro-batches otherwise produce many tiny files.
         */
        boolean optimizeWrite,

        /**
         * When {@code true}, Delta automatically compacts small files after each
         * micro-batch. Useful when {@code optimizeWrite} alone is insufficient, at the
         * cost of extra write amplification per batch.
         */
        boolean autoCompact,

        /**
         * When {@code true}, Delta merges incoming columns into the existing table schema
         * on each write. Disabled by default to prevent accidental schema drift; enable
         * only when the upstream Avro schema is known to evolve in a backward-compatible
         * way.
         */
        boolean mergeSchema,

        /**
         * Maximum number of records written to a single Parquet file. When {@code null},
         * Spark does not enforce a per-file record limit. Setting this caps the row group
         * size, which can improve read performance for narrow queries but may produce
         * many small files if set too low.
         */
        @Min(value = 1, message = "DELTA_MAX_RECORDS_PER_FILE must be at least 1")
        Long maxRecordsPerFile) {

    /**
     * Returns a default {@code DeltaSinkConfig}: no partitioning, Delta-default file
     * size, optimized writes enabled, no auto-compaction, no schema evolution.
     *
     * @return a safe default tuning configuration
     */
    public static DeltaSinkConfig defaults() {
        return new DeltaSinkConfig(List.of(), null, true, false, false, null);
    }
}
