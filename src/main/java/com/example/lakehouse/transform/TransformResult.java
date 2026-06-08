package com.example.lakehouse.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Holds the two output streams produced by a {@link Transformer}: rows that
 * conform to the target schema ({@code valid}), and rows that failed JSON parsing
 * routed to the dead-letter path ({@code deadLetter}).
 *
 * @author Alexander Castro
 * @see Transformer
 */
public record TransformResult(Dataset<Row> valid, Dataset<Row> deadLetter) {}
