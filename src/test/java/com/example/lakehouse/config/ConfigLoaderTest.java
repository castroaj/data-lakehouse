package com.example.lakehouse.config;

import com.example.lakehouse.exception.ConfigurationException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.example.lakehouse.config.ConfigLoader.AWS_REGION;
import static com.example.lakehouse.config.ConfigLoader.CHECKPOINT_PATH;
import static com.example.lakehouse.config.ConfigLoader.DEAD_LETTER_PATH;
import static com.example.lakehouse.config.ConfigLoader.DELTA_TABLE_PATH;
import static com.example.lakehouse.config.ConfigLoader.KAFKA_BOOTSTRAP_SERVERS;
import static com.example.lakehouse.config.ConfigLoader.KAFKA_GROUP_ID;
import static com.example.lakehouse.config.ConfigLoader.KAFKA_MAX_OFFSETS_PER_TRIGGER;
import static com.example.lakehouse.config.ConfigLoader.KAFKA_STARTING_OFFSETS;
import static com.example.lakehouse.config.ConfigLoader.KAFKA_TOPIC;
import static com.example.lakehouse.config.ConfigLoader.S3_BUCKET_NAME;
import static com.example.lakehouse.config.ConfigLoader.SPARK_APP_NAME;
import static com.example.lakehouse.config.ConfigLoader.SPARK_MASTER;
import static com.example.lakehouse.config.ConfigLoader.TRIGGER_INTERVAL_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ConfigLoaderTest {

    private static final Map<String, String> VALID_ENV = Map.of(
            KAFKA_BOOTSTRAP_SERVERS, "broker:9092",
            KAFKA_TOPIC, "events",
            KAFKA_GROUP_ID, "lakehouse-consumer",
            S3_BUCKET_NAME, "my-bucket",
            AWS_REGION, "us-east-1",
            DELTA_TABLE_PATH, "lakehouse/events",
            CHECKPOINT_PATH, "checkpoints/events",
            DEAD_LETTER_PATH, "dead-letter/events");

    private final ConfigLoader loader = new ConfigLoader();

    @Nested
    class LoadsConfig {

        @Test
        void loadsAllRequiredFields() {
            IngestionConfig config = loader.load(VALID_ENV::get);

            assertThat(config.kafkaBootstrapServers()).isEqualTo("broker:9092");
            assertThat(config.kafkaTopic()).isEqualTo("events");
            assertThat(config.kafkaGroupId()).isEqualTo("lakehouse-consumer");
            assertThat(config.s3BucketName()).isEqualTo("my-bucket");
            assertThat(config.s3Region()).isEqualTo("us-east-1");
            assertThat(config.deltaTablePath()).isEqualTo("lakehouse/events");
            assertThat(config.checkpointPath()).isEqualTo("checkpoints/events");
            assertThat(config.deadLetterPath()).isEqualTo("dead-letter/events");
        }

        @Test
        void appliesDefaultsForOptionalFields() {
            IngestionConfig config = loader.load(VALID_ENV::get);

            assertThat(config.kafkaStartingOffsets()).isEqualTo("latest");
            assertThat(config.kafkaMaxOffsetsPerTrigger()).isEqualTo(100_000L);
            assertThat(config.triggerIntervalSeconds()).isEqualTo(30);
            assertThat(config.sparkMaster()).isEqualTo("local[*]");
            assertThat(config.sparkAppName()).isEqualTo("lakehouse-ingestion");
        }

        @Test
        void loadsFromSystemPropertiesWhenEnvVarAbsent() {
            VALID_ENV.forEach(System::setProperty);
            try {
                IngestionConfig config = loader.load();
                assertThat(config.kafkaBootstrapServers()).isEqualTo("broker:9092");
                assertThat(config.kafkaTopic()).isEqualTo("events");
            } finally {
                VALID_ENV.keySet().forEach(System::clearProperty);
            }
        }

        @Test
        void overridesDefaultsWhenOptionalFieldsProvided() {
            Map<String, String> env = new HashMap<>(VALID_ENV);
            env.put(KAFKA_STARTING_OFFSETS, "earliest");
            env.put(KAFKA_MAX_OFFSETS_PER_TRIGGER, "50000");
            env.put(TRIGGER_INTERVAL_SECONDS, "60");
            env.put(SPARK_MASTER, "local[4]");
            env.put(SPARK_APP_NAME, "my-app");

            IngestionConfig config = loader.load(env::get);

            assertThat(config.kafkaStartingOffsets()).isEqualTo("earliest");
            assertThat(config.kafkaMaxOffsetsPerTrigger()).isEqualTo(50_000L);
            assertThat(config.triggerIntervalSeconds()).isEqualTo(60);
            assertThat(config.sparkMaster()).isEqualTo("local[4]");
            assertThat(config.sparkAppName()).isEqualTo("my-app");
        }
    }

    @Nested
    class RequiredFieldValidation {

        @ParameterizedTest
        @MethodSource("com.example.lakehouse.config.ConfigLoaderTest#requiredFields")
        void throwsWhenRequiredFieldMissing(String envKey, String expectedFieldName) {
            assertConfigException(without(envKey), expectedFieldName);
        }

        @Test
        void throwsWhenMultipleRequiredFieldsMissing() {
            Map<String, String> env = new HashMap<>(VALID_ENV);
            env.remove(KAFKA_TOPIC);
            env.remove(S3_BUCKET_NAME);

            assertThatThrownBy(() -> loader.load(env::get))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("kafkaTopic")
                    .hasMessageContaining("s3BucketName");
        }
    }

    @Nested
    class PathValidation {

        @ParameterizedTest
        @CsvSource({
                "DELTA_TABLE_PATH, deltaTablePath",
                "CHECKPOINT_PATH, checkpointPath",
                "DEAD_LETTER_PATH, deadLetterPath"
        })
        void throwsWhenPathHasS3aPrefix(String envKey, String expectedFieldName) {
            Map<String, String> env = new HashMap<>(VALID_ENV);
            env.put(envKey, "s3a://my-bucket/path");
            assertConfigException(env, expectedFieldName);
        }
    }

    @Nested
    class NumericValidation {

        @ParameterizedTest
        @CsvSource({
                "TRIGGER_INTERVAL_SECONDS, 0, triggerIntervalSeconds",
                "TRIGGER_INTERVAL_SECONDS, -5, triggerIntervalSeconds",
                "KAFKA_MAX_OFFSETS_PER_TRIGGER, 0, kafkaMaxOffsetsPerTrigger"
        })
        void throwsWhenValueBelowMinimum(String envKey, String value, String expectedFieldName) {
            Map<String, String> env = new HashMap<>(VALID_ENV);
            env.put(envKey, value);
            assertConfigException(env, expectedFieldName);
        }

        @Test
        void triggerIntervalOfOneIsAccepted() {
            Map<String, String> env = new HashMap<>(VALID_ENV);
            env.put(TRIGGER_INTERVAL_SECONDS, "1");
            IngestionConfig config = loader.load(env::get);
            assertThat(config.triggerIntervalSeconds()).isEqualTo(1);
        }

        @ParameterizedTest
        @CsvSource({
                "TRIGGER_INTERVAL_SECONDS, thirty",
                "KAFKA_MAX_OFFSETS_PER_TRIGGER, many"
        })
        void throwsWhenValueIsNotANumber(String envKey, String value) {
            Map<String, String> env = new HashMap<>(VALID_ENV);
            env.put(envKey, value);
            assertThatThrownBy(() -> loader.load(env::get))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(envKey)
                    .hasMessageContaining(value);
        }
    }

    static Stream<org.junit.jupiter.params.provider.Arguments> requiredFields() {
        return Stream.of(
                arguments(KAFKA_BOOTSTRAP_SERVERS, "kafkaBootstrapServers"),
                arguments(KAFKA_TOPIC, "kafkaTopic"),
                arguments(KAFKA_GROUP_ID, "kafkaGroupId"),
                arguments(S3_BUCKET_NAME, "s3BucketName"),
                arguments(AWS_REGION, "s3Region"),
                arguments(DELTA_TABLE_PATH, "deltaTablePath"),
                arguments(CHECKPOINT_PATH, "checkpointPath"),
                arguments(DEAD_LETTER_PATH, "deadLetterPath"));
    }

    private void assertConfigException(Map<String, String> env, String fieldName) {
        assertThatThrownBy(() -> loader.load(env::get))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(fieldName);
    }

    private static Map<String, String> without(String key) {
        Map<String, String> env = new HashMap<>(VALID_ENV);
        env.remove(key);
        return env;
    }
}
