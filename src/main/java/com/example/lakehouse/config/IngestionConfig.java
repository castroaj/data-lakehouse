package com.example.lakehouse.config;

import com.example.lakehouse.ingestion.kafka.KafkaSourceConfig;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Configuration for the ingestion job, loaded from environment variables or
 * system properties
 *
 * @author Alexander Castro
 */
public record IngestionConfig(
        @Valid KafkaSourceConfig kafkaSourceConfig,
        @NotBlank(message = "S3_BUCKET_NAME must not be blank") String s3BucketName,
        @NotBlank(message = "AWS_REGION must not be blank") String s3Region,
        @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "DELTA_TABLE_PATH must be a relative path, not an s3a:// URI") String deltaTablePath,
        @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "CHECKPOINT_PATH must be a relative path, not an s3a:// URI") String checkpointPath,
        @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "DEAD_LETTER_PATH must be a relative path, not an s3a:// URI") String deadLetterPath,
        @Min(value = 1, message = "TRIGGER_INTERVAL_SECONDS must be at least 1") int triggerIntervalSeconds,
        String sparkMaster,
        String sparkAppName,
        @Valid MetricsConfig metricsConfig) {
}
