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
        @NotBlank String s3BucketName,
        @NotBlank String s3Region,
        @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "must be a relative path, not an s3a:// URI") String deltaTablePath,
        @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "must be a relative path, not an s3a:// URI") String checkpointPath,
        @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "must be a relative path, not an s3a:// URI") String deadLetterPath,
        @Min(1) int triggerIntervalSeconds,
        String sparkMaster,
        String sparkAppName) {
}
