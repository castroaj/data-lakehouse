package com.example.lakehouse.ingestion.kafka;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Configuration record for the Kafka consumer, holding all settings required
 * to connect to a broker and consume a topic.
 *
 * @author Alexander Castro
 */
public record KafkaSourceConfig(
        @NotBlank(message = "KAFKA_BOOTSTRAP_SERVERS must not be blank") String bootstrapServers,
        @NotBlank(message = "KAFKA_TOPIC must not be blank") String topic,
        @NotBlank(message = "KAFKA_GROUP_ID must not be blank") String groupId,
        @Pattern(regexp = "^(earliest|latest)$", message = "startingOffsets must be either 'earliest' or 'latest'") String startingOffsets,
        long maxOffsetsPerTrigger) {
}
