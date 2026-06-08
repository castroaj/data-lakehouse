package com.example.lakehouse.sink;

import com.example.lakehouse.config.DeltaSinkConfig;
import com.example.lakehouse.transform.TransformResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for {@link DeltaSink} lifecycle and option-forwarding behaviour.
 * Uses a local SparkSession with Delta extensions and a {@link TempDir} to
 * avoid any S3 dependency.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeltaSinkTest {

    private SparkSession spark;

    @BeforeAll
    void setUpSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("DeltaSinkTest")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    void tearDownSpark() {
        if (spark != null) spark.stop();
    }

    // AC-8: close() before write() is safe — no queries have been started
    @Test
    void close_beforeWrite_doesNotThrow(@TempDir Path tempDir) {
        DeltaSink sink = buildSink(tempDir, DeltaSinkConfig.defaults());

        assertThatCode(sink::close).doesNotThrowAnyException();
    }

    // AC-7 + AC-9: default config starts two queries; close() stops them both
    @Test
    void write_withDefaultConfig_startsTwoActiveQueries(@TempDir Path tempDir) throws Exception {
        DeltaSink sink = buildSink(tempDir, DeltaSinkConfig.defaults());
        TransformResult result = buildRateResult();

        List<StreamingQuery> queries = sink.write(result);

        assertThat(queries).hasSize(2);
        assertThat(queries.get(0).isActive()).isTrue();
        assertThat(queries.get(1).isActive()).isTrue();

        sink.close();
    }

    // AC-9: close() after write() stops all queries
    @Test
    void close_afterWrite_stopsAllQueries(@TempDir Path tempDir) throws Exception {
        DeltaSink sink = buildSink(tempDir, DeltaSinkConfig.defaults());
        List<StreamingQuery> queries = sink.write(buildRateResult());

        sink.close();

        assertThat(queries.get(0).isActive()).isFalse();
        assertThat(queries.get(1).isActive()).isFalse();
    }

    // AC-10: calling close() twice does not throw
    @Test
    void close_calledTwice_doesNotThrow(@TempDir Path tempDir) throws Exception {
        DeltaSink sink = buildSink(tempDir, DeltaSinkConfig.defaults());
        sink.write(buildRateResult());

        sink.close();
        assertThatCode(sink::close).doesNotThrowAnyException();
    }

    // AC-1: partition columns → queries start successfully
    @Test
    void write_withPartitionColumns_startsSuccessfully(@TempDir Path tempDir) throws Exception {
        DeltaSinkConfig cfg = new DeltaSinkConfig(
                List.of("value"), null, true, false, false, null);
        DeltaSink sink = buildSink(tempDir, cfg);

        // Rate stream produces "value" column — valid partition key
        List<StreamingQuery> queries = sink.write(buildRateResult());

        assertThat(queries).hasSize(2);
        assertThat(queries.get(0).isActive()).isTrue();

        sink.close();
    }

    // AC-2, AC-3, AC-4, AC-5, AC-6: all tuning options enabled — queries start without error
    @Test
    void write_withAllTuningOptionsEnabled_startsSuccessfully(@TempDir Path tempDir) throws Exception {
        DeltaSinkConfig cfg = new DeltaSinkConfig(
                List.of(), 67_108_864L, true, true, true, 500_000L);
        DeltaSink sink = buildSink(tempDir, cfg);

        List<StreamingQuery> queries = sink.write(buildRateResult());

        assertThat(queries).hasSize(2);
        assertThat(queries.get(0).isActive()).isTrue();

        sink.close();
    }

    // Builds a DeltaSink using the package-private constructor with local temp paths.
    private DeltaSink buildSink(Path tempDir, DeltaSinkConfig cfg) {
        String deltaPath = tempDir.resolve("delta").toAbsolutePath().toString();
        String checkpointBase = tempDir.resolve("checkpoints").toAbsolutePath().toString();
        String deadLetterPath = tempDir.resolve("dead-letter").toAbsolutePath().toString();
        StreamingQueryListener listener = Mockito.mock(StreamingQueryListener.class);
        return new DeltaSink(spark, deltaPath, checkpointBase, deadLetterPath, 1, cfg, listener);
    }

    // Rate source produces (timestamp, value) — works for both Delta and JSON writes.
    private TransformResult buildRateResult() {
        Dataset<Row> stream = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", "10")
                .load();
        return new TransformResult(stream, stream);
    }
}
