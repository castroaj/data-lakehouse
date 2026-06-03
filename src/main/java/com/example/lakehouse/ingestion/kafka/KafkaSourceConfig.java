package com.example.lakehouse.ingestion.kafka;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record KafkaSourceConfig(
        @NotBlank String bootstrapServers,
        @NotBlank String topic,
        @NotBlank String groupId,
        @Pattern(regexp = "^(earliest|latest)$", message = "startingOffsets must be either 'earliest' or 'latest'") String startingOffsets,
        long maxOffsetsPerTrigger) {
}
