package com.example.lakehouse.config;

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
                @NotBlank String kafkaBootstrapServers,
                @NotBlank String kafkaTopic,
                @NotBlank String kafkaGroupId,
                String kafkaStartingOffsets,
                @Min(1) long kafkaMaxOffsetsPerTrigger,
                @NotBlank String s3BucketName,
                @NotBlank String s3Region,
                @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "must be a relative path, not an s3a:// URI") String deltaTablePath,
                @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "must be a relative path, not an s3a:// URI") String checkpointPath,
                @NotBlank @Pattern(regexp = "^(?!s3a://).*", message = "must be a relative path, not an s3a:// URI") String deadLetterPath,
                @Min(1) int triggerIntervalSeconds,
                String sparkMaster,
                String sparkAppName) {
}
