package com.example.lakehouse.config;

import jakarta.validation.constraints.Min;

/**
 * Configuration record for the Prometheus metrics HTTP server.
 *
 * @author Alexander Castro
 */
public record MetricsConfig(
        @Min(value = 1024, message = "METRICS_PORT must be at least 1024") int metricsPort) {
}
