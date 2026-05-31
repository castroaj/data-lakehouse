package com.example.lakehouse.config;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hibernate.validator.HibernateValidator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;

import com.example.lakehouse.exception.ConfigurationException;

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
        IngestionConfig config = new IngestionConfig(
                env.apply(KAFKA_BOOTSTRAP_SERVERS),
                env.apply(KAFKA_TOPIC),
                env.apply(KAFKA_GROUP_ID),
                defaultString(env.apply(KAFKA_STARTING_OFFSETS), "latest"),
                parseLong(KAFKA_MAX_OFFSETS_PER_TRIGGER, env.apply(KAFKA_MAX_OFFSETS_PER_TRIGGER), 100_000L),
                env.apply(S3_BUCKET_NAME),
                env.apply(AWS_REGION),
                env.apply(DELTA_TABLE_PATH),
                env.apply(CHECKPOINT_PATH),
                env.apply(DEAD_LETTER_PATH),
                parseInt(TRIGGER_INTERVAL_SECONDS, env.apply(TRIGGER_INTERVAL_SECONDS), 30),
                defaultString(env.apply(SPARK_MASTER), "local[*]"),
                defaultString(env.apply(SPARK_APP_NAME), "lakehouse-ingestion"));

        Set<ConstraintViolation<IngestionConfig>> violations = VALIDATOR.validate(config);
        if (!violations.isEmpty()) {
            String details = violations.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new ConfigurationException("Configuration validation failed: " + details);
        }

        return config;
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
}
