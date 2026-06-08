package com.example.lakehouse.config;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hibernate.validator.HibernateValidator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;

import com.example.lakehouse.exception.ConfigurationException;
import com.example.lakehouse.ingestion.kafka.KafkaSourceConfig;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;

/**
 * Loads configuration from environment variables and validates it using Jakarta
 * Bean Validation (Hibernate Validator)
 *
 * @author Alexander Castro
 */
public class ConfigLoader {

    private static final String DEFAULT_STARTING_OFFSETS = "latest";
    private static final long DEFAULT_MAX_OFFSETS_PER_TRIGGER = 100_000L;
    private static final int DEFAULT_METRICS_PORT = 9090;

    public static final String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
    public static final String KAFKA_TOPIC = "KAFKA_TOPIC";
    public static final String KAFKA_GROUP_ID = "KAFKA_GROUP_ID";
    public static final String KAFKA_STARTING_OFFSETS = "KAFKA_STARTING_OFFSETS";
    public static final String KAFKA_MAX_OFFSETS_PER_TRIGGER = "KAFKA_MAX_OFFSETS_PER_TRIGGER";
    public static final String S3_BUCKET_NAME = "S3_BUCKET_NAME";
    public static final String AWS_REGION = "AWS_REGION";
    public static final String DELTA_TABLE_PATH = "DELTA_TABLE_PATH";
    public static final String CHECKPOINT_PATH = "CHECKPOINT_PATH";
    public static final String DEAD_LETTER_PATH = "DEAD_LETTER_PATH";
    public static final String TRIGGER_INTERVAL_SECONDS = "TRIGGER_INTERVAL_SECONDS";
    public static final String SPARK_MASTER = "SPARK_MASTER";
    public static final String SPARK_APP_NAME = "SPARK_APP_NAME";
    public static final String METRICS_PORT = "METRICS_PORT";

    public static final String DELTA_PARTITION_COLUMNS = "DELTA_PARTITION_COLUMNS";
    public static final String DELTA_TARGET_FILE_SIZE_BYTES = "DELTA_TARGET_FILE_SIZE_BYTES";
    public static final String DELTA_OPTIMIZE_WRITE = "DELTA_OPTIMIZE_WRITE";
    public static final String DELTA_AUTO_COMPACT = "DELTA_AUTO_COMPACT";
    public static final String DELTA_MERGE_SCHEMA = "DELTA_MERGE_SCHEMA";
    public static final String DELTA_MAX_RECORDS_PER_FILE = "DELTA_MAX_RECORDS_PER_FILE";

    // Reuse a single Validator instance since they are thread-safe and expensive to
    // create
    private static final Validator VALIDATOR = Validation.byProvider(HibernateValidator.class)
            .configure()
            .messageInterpolator(new ParameterMessageInterpolator())
            .buildValidatorFactory()
            .getValidator();

    /**
     * Loads configuration from environment variables and system properties,
     * applying defaults for optional fields, and validates the resulting
     * configuration
     * 
     * @return validated IngestionConfig instance
     * @throws ConfigurationException if validation fails
     */
    public IngestionConfig load() {
        return load(key -> {
            String val = System.getenv(key);
            return val != null ? val : System.getProperty(key);
        });
    }

    /**
     * Loads configuration using the provided function to resolve keys. Utilizing
     * environment variables and system properties by default, but allowing
     * overrides for testing.
     * 
     * @param env function to resolve configuration keys
     * @return validated IngestionConfig instance
     * @throws ConfigurationException if validation fails
     */
    IngestionConfig load(Function<String, String> env) {

        var kafkaSourceConfig = new KafkaSourceConfig(
                env.apply(KAFKA_BOOTSTRAP_SERVERS),
                env.apply(KAFKA_TOPIC),
                env.apply(KAFKA_GROUP_ID),
                defaultString(env.apply(KAFKA_STARTING_OFFSETS), DEFAULT_STARTING_OFFSETS),
                parseLong(KAFKA_MAX_OFFSETS_PER_TRIGGER, env.apply(KAFKA_MAX_OFFSETS_PER_TRIGGER),
                        DEFAULT_MAX_OFFSETS_PER_TRIGGER));

        var metricsConfig = new MetricsConfig(
                parseInt(METRICS_PORT, env.apply(METRICS_PORT), DEFAULT_METRICS_PORT));

        var deltaSinkConfig = new DeltaSinkConfig(
                parsePartitionColumns(env.apply(DELTA_PARTITION_COLUMNS)),
                parseOptionalLong(DELTA_TARGET_FILE_SIZE_BYTES, env.apply(DELTA_TARGET_FILE_SIZE_BYTES)),
                parseBoolean(env.apply(DELTA_OPTIMIZE_WRITE), true),
                parseBoolean(env.apply(DELTA_AUTO_COMPACT), false),
                parseBoolean(env.apply(DELTA_MERGE_SCHEMA), false),
                parseOptionalLong(DELTA_MAX_RECORDS_PER_FILE, env.apply(DELTA_MAX_RECORDS_PER_FILE)));

        IngestionConfig ingestionConfig = new IngestionConfig(
                kafkaSourceConfig,
                env.apply(S3_BUCKET_NAME),
                env.apply(AWS_REGION),
                env.apply(DELTA_TABLE_PATH),
                env.apply(CHECKPOINT_PATH),
                env.apply(DEAD_LETTER_PATH),
                parseInt(TRIGGER_INTERVAL_SECONDS, env.apply(TRIGGER_INTERVAL_SECONDS), 30),
                defaultString(env.apply(SPARK_MASTER), "local[*]"),
                defaultString(env.apply(SPARK_APP_NAME), "lakehouse-ingestion"),
                metricsConfig,
                deltaSinkConfig);

        Set<ConstraintViolation<IngestionConfig>> violations = VALIDATOR.validate(ingestionConfig);
        if (!violations.isEmpty()) {
            String details = violations.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new ConfigurationException("Configuration validation failed: " + details);
        }

        return ingestionConfig;
    }

    /**
     * Returns the provided value if it's not null, otherwise returns the default
     * value
     * 
     * @param value        the value to check for null
     * @param defaultValue the value to return if the provided value is null
     * @return the provided value if it's not null, otherwise the default value
     */
    private static String defaultString(String value, String defaultValue) {
        return value != null ? value : defaultValue;
    }

    /**
     * Parses the provided string as an integer, returning the default value if the
     * string is null, and throwing a ConfigurationException if the string is not a
     * valid integer
     * 
     * @param field        the name of the configuration field being parsed (for
     *                     error messages)
     * @param value        the string value to parse as an integer
     * @param defaultValue the value to return if the provided string is null
     * @return the parsed integer value, or the default value if the provided string
     *         is null
     * @throws ConfigurationException if the value is not a valid integer
     */
    private static int parseInt(String field, String value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ConfigurationException(
                    "Configuration validation failed: " + field + ": invalid integer value '" + value + "'");
        }
    }

    /**
     * Parses the provided string as a long, returning the default value if the
     * string is null, and throwing a ConfigurationException if the string is not a
     * valid long
     *
     * @param field        the name of the configuration field being parsed (for
     *                     error messages)
     * @param value        the string value to parse as a long
     * @param defaultValue the value to return if the provided string is null
     * @return the parsed long value, or the default value if the provided string
     *         is null
     * @throws ConfigurationException if the value is not a valid long
     */
    private static long parseLong(String field, String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new ConfigurationException(
                    "Configuration validation failed: " + field + ": invalid long value '" + value + "'");
        }
    }

    /**
     * Parses the provided string as a boxed {@code Long}, returning {@code null}
     * when the string is absent. Used for optional numeric DeltaSinkConfig fields
     * where the absence of a value means "use the Delta/Spark default".
     *
     * @param field the configuration field name; included in the error message
     * @param value the raw env-var string, or {@code null} when unset
     * @return the parsed value, or {@code null} if {@code value} is {@code null}
     * @throws ConfigurationException if the string is present but not a valid long
     */
    private static Long parseOptionalLong(String field, String value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new ConfigurationException(
                    "Configuration validation failed: " + field + ": invalid long value '" + value + "'");
        }
    }

    /**
     * Parses a comma-separated list of column names from the env var value.
     * Blank entries produced by leading/trailing commas or double-commas are
     * silently removed. Returns an empty list when the value is absent or blank.
     *
     * @param value the raw env-var string, or {@code null} when unset
     * @return an unmodifiable list of trimmed, non-blank column names
     */
    private static List<String> parsePartitionColumns(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    /**
     * Parses a boolean env-var value, returning {@code defaultValue} when the
     * string is absent. Delegates to {@link Boolean#parseBoolean}, which treats
     * any string other than (case-insensitive) {@code "true"} as {@code false}.
     *
     * @param value        the raw env-var string, or {@code null} when unset
     * @param defaultValue the value to use when the env var is absent
     * @return the parsed or default boolean
     */
    private static boolean parseBoolean(String value, boolean defaultValue) {
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }
}
