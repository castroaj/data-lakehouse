package com.example.lakehouse.ingestion.kafka;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaSourceTest {

    private static final KafkaSourceConfig CONFIG = new KafkaSourceConfig(
            "localhost:9092", "events", "test-group", "latest", 100_000L);

    private SparkSession spark;
    private KafkaSource source;

    @BeforeAll
    void createSession() {
        spark = SparkSession.builder()
                .appName("KafkaSourceTest")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    void closeSession() {
        if (spark != null) {
            spark.stop();
        }
    }

    @BeforeEach
    void createSource() {
        source = new KafkaSource(spark, CONFIG);
    }

    @Nested
    class ReadStream {

        @Test
        void returnsStreamingDataset() {
            Dataset<Row> ds = source.readStream();

            assertThat(ds.isStreaming()).isTrue();
            List<String> columns = Arrays.asList(ds.schema().fieldNames());
            assertThat(columns).contains("key", "value", "topic", "partition", "offset", "timestamp", "timestampType");
        }

        @Test
        void throwsWhenClosed() {
            source.close();

            assertThatThrownBy(() -> source.readStream())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("events");
        }
    }

    @Nested
    class ReadBatch {

        @Test
        void throwsWhenClosed() {
            source.close();

            // closed check fires before null check, so null is safe to pass here
            assertThatThrownBy(() -> source.readBatch(null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("events");
        }

        @Test
        void throwsForNullRange() {
            assertThatThrownBy(() -> source.readBatch(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Close {

        @Test
        void isIdempotent() {
            source.close();
            source.close(); // must not throw
        }
    }
}
