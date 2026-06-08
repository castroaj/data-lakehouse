package com.example.lakehouse.config;

import com.example.lakehouse.exception.ConfigurationException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.example.lakehouse.config.ConfigLoader.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link DeltaSinkConfig} defaults, ConfigLoader parsing, and
 * Bean Validation constraints wired through {@link IngestionConfig}.
 */
class DeltaSinkConfigTest {

    private static final Map<String, String> BASE_ENV = Map.of(
            KAFKA_BOOTSTRAP_SERVERS, "broker:9092",
            KAFKA_TOPIC, "events",
            KAFKA_GROUP_ID, "lakehouse-consumer",
            S3_BUCKET_NAME, "my-bucket",
            AWS_REGION, "us-east-1",
            DELTA_TABLE_PATH, "lakehouse/events",
            CHECKPOINT_PATH, "checkpoints/events",
            DEAD_LETTER_PATH, "dead-letter/events");

    private final ConfigLoader loader = new ConfigLoader();

    // AC-7: defaults()
    @Test
    void defaults_hasOptimizeWriteEnabledAndNoPartitioning() {
        DeltaSinkConfig cfg = DeltaSinkConfig.defaults();

        assertThat(cfg.partitionColumns()).isEmpty();
        assertThat(cfg.targetFileSizeBytes()).isNull();
        assertThat(cfg.optimizeWrite()).isTrue();
        assertThat(cfg.autoCompact()).isFalse();
        assertThat(cfg.mergeSchema()).isFalse();
        assertThat(cfg.maxRecordsPerFile()).isNull();
    }

    @Nested
    class ConfigLoaderParsing {

        // AC-11: defaults when no env vars set
        @Test
        void loadsDefaults_whenDeltaEnvVarsAbsent() {
            IngestionConfig config = loader.load(BASE_ENV::get);

            DeltaSinkConfig sink = config.deltaSinkConfig();
            assertThat(sink.partitionColumns()).isEmpty();
            assertThat(sink.targetFileSizeBytes()).isNull();
            assertThat(sink.optimizeWrite()).isTrue();
            assertThat(sink.autoCompact()).isFalse();
            assertThat(sink.mergeSchema()).isFalse();
            assertThat(sink.maxRecordsPerFile()).isNull();
        }

        // AC-11: partition columns parsed from comma-separated env var
        @Test
        void parsesPartitionColumns_fromCommaSeparatedValue() {
            Map<String, String> env = envWith(DELTA_PARTITION_COLUMNS, "eventType, date");

            DeltaSinkConfig sink = loader.load(env::get).deltaSinkConfig();

            assertThat(sink.partitionColumns()).containsExactly("eventType", "date");
        }

        // AC-11: blank entries in partition columns are stripped
        @Test
        void parsesPartitionColumns_stripsBlankEntries() {
            Map<String, String> env = envWith(DELTA_PARTITION_COLUMNS, "eventType,,date");

            DeltaSinkConfig sink = loader.load(env::get).deltaSinkConfig();

            assertThat(sink.partitionColumns()).containsExactly("eventType", "date");
        }

        // AC-11: numeric fields parsed correctly
        @Test
        void parsesNumericFields() {
            Map<String, String> env = new HashMap<>(BASE_ENV);
            env.put(DELTA_TARGET_FILE_SIZE_BYTES, "67108864");
            env.put(DELTA_MAX_RECORDS_PER_FILE, "1000000");

            DeltaSinkConfig sink = loader.load(env::get).deltaSinkConfig();

            assertThat(sink.targetFileSizeBytes()).isEqualTo(67_108_864L);
            assertThat(sink.maxRecordsPerFile()).isEqualTo(1_000_000L);
        }

        // AC-11: boolean flags
        @Test
        void parsesBooleanFlags() {
            Map<String, String> env = new HashMap<>(BASE_ENV);
            env.put(DELTA_OPTIMIZE_WRITE, "false");
            env.put(DELTA_AUTO_COMPACT, "true");
            env.put(DELTA_MERGE_SCHEMA, "true");

            DeltaSinkConfig sink = loader.load(env::get).deltaSinkConfig();

            assertThat(sink.optimizeWrite()).isFalse();
            assertThat(sink.autoCompact()).isTrue();
            assertThat(sink.mergeSchema()).isTrue();
        }

        // AC-12: negative targetFileSizeBytes → ConfigurationException
        @Test
        void rejectsNegativeTargetFileSizeBytes() {
            Map<String, String> env = envWith(DELTA_TARGET_FILE_SIZE_BYTES, "-1");

            assertThatThrownBy(() -> loader.load(env::get))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("DELTA_TARGET_FILE_SIZE_BYTES");
        }

        // AC-12: negative maxRecordsPerFile → ConfigurationException
        @Test
        void rejectsNegativeMaxRecordsPerFile() {
            Map<String, String> env = envWith(DELTA_MAX_RECORDS_PER_FILE, "-100");

            assertThatThrownBy(() -> loader.load(env::get))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("DELTA_MAX_RECORDS_PER_FILE");
        }

        // AC-12: non-parseable long → ConfigurationException
        @Test
        void rejectsNonNumericTargetFileSize() {
            Map<String, String> env = envWith(DELTA_TARGET_FILE_SIZE_BYTES, "big");

            assertThatThrownBy(() -> loader.load(env::get))
                    .isInstanceOf(ConfigurationException.class);
        }
    }

    private Map<String, String> envWith(String key, String value) {
        Map<String, String> env = new HashMap<>(BASE_ENV);
        env.put(key, value);
        return env;
    }
}
