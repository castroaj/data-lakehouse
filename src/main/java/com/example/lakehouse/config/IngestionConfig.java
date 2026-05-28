package com.example.lakehouse.config;

public record IngestionConfig(
        String kafkaBootstrapServers,
        String kafkaTopic,
        String kafkaGroupId,
        String kafkaStartingOffsets,
        long kafkaMaxOffsetsPerTrigger,
        String s3BucketName,
        String s3Region,
        String deltaTablePath,
        String checkpointPath,
        String deadLetterPath,
        int triggerIntervalSeconds,
        String sparkMaster,
        String sparkAppName
) {
    // TODO: add compact constructor with validation (throws ConfigurationException)
}
