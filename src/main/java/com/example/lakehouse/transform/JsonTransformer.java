package com.example.lakehouse.transform;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.from_json;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.example.lakehouse.transform.schema.SchemaProvider;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Transforms a raw Kafka {@link Dataset} into valid and dead-letter partitions
 * by parsing each message's JSON value against the topic's Avro-derived schema.
 *
 * @author Alexander Castro
 * @see Transformer
 */
public class JsonTransformer implements Transformer {

        // from_json uses this sentinel column to capture rows that fail JSON parsing
        // without throwing; rows with a non-null value here are routed to dead-letter.
        private static final String CORRUPT_RECORD_COL = "_corrupt_record";

        /** Provides the Spark StructType for the configured topic. */
        private final SchemaProvider schemaProvider;

        /** The Kafka topic name whose schema is used for JSON parsing. */
        private final String topic;

        /**
         * Constructs a JsonTransformer for the given topic using schemas from the
         * provided provider.
         *
         * @param schemaProvider the schema provider that resolves the topic's StructType; must not be null
         * @param topic          the Kafka topic name whose schema will be used for parsing; must not be blank
         * @param registry       the meter registry; reserved for future per-transform instrumentation
         */
        public JsonTransformer(SchemaProvider schemaProvider, String topic, MeterRegistry registry) {
                this.schemaProvider = schemaProvider;
                this.topic = topic;
        }

        /**
         * Parses each Kafka message's JSON value against the topic schema, routing
         * conforming rows to {@code valid} and rows that fail parsing to
         * {@code deadLetter}.
         *
         * <p>Kafka metadata columns ({@code _kafka_offset}, {@code _kafka_partition})
         * are preserved on both output streams. Valid rows also carry an
         * {@code _ingested_at} watermark timestamp added at ingestion time.
         *
         * @param input the raw Kafka streaming dataset with a binary {@code value} column
         * @return a TransformResult containing the valid and dead-letter partitions; never null
         */
        @Override
        public TransformResult transform(Dataset<Row> input) {
                StructType schema = schemaProvider.schemaFor(topic);
                // Append the corrupt-record sentinel so from_json can populate it on parse
                // failure rather than silently dropping the offending row.
                StructType schemaWithCorrupt = schema.add(CORRUPT_RECORD_COL, DataTypes.StringType, true);

                var parsed = input.select(
                                from_json(
                                                col("value").cast("string"),
                                                schemaWithCorrupt,
                                                Map.of("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)).alias("data"),
                                col("offset").alias("_kafka_offset"),
                                col("partition").alias("_kafka_partition"))
                                .select("data.*", "_kafka_offset", "_kafka_partition");

                var valid = parsed.filter(col(CORRUPT_RECORD_COL).isNull())
                                .drop(CORRUPT_RECORD_COL)
                                // _ingested_at marks when the record entered the lake; useful for
                                // lag monitoring and late-data detection downstream.
                                .withColumn("_ingested_at", current_timestamp());

                var deadLetter = parsed.filter(col(CORRUPT_RECORD_COL).isNotNull())
                                .drop(CORRUPT_RECORD_COL);

                return new TransformResult(valid, deadLetter);
        }
}
