package com.example.lakehouse.sink;

import com.example.lakehouse.transform.TransformResult;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.List;

/**
 * Writes a {@link TransformResult} to persistent storage as one or more
 * Structured Streaming queries.
 *
 * @author Alexander Castro
 */
public interface Sink extends AutoCloseable {

    /**
     * Starts the streaming queries that persist the transform result. Implementations
     * must start at least one query for valid records and may start additional
     * queries for dead-letter records.
     *
     * @param result the split transform result to write; must not be null
     * @return the list of started streaming queries; never null or empty
     */
    List<StreamingQuery> write(TransformResult result);

    /**
     * Stops all active streaming queries. Must be idempotent; calling close before
     * write, or calling it multiple times, must be safe.
     */
    @Override
    default void close() {}
}
