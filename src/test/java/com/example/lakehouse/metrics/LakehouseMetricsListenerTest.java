package com.example.lakehouse.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LakehouseMetricsListenerTest {

    @Mock
    private QueryProgressEvent validProgressEvent;

    @Mock
    private StreamingQueryProgress validProgress;

    @Mock
    private QueryProgressEvent deadLetterProgressEvent;

    @Mock
    private StreamingQueryProgress deadLetterProgress;

    private SimpleMeterRegistry registry;
    private LakehouseMetricsListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        listener = new LakehouseMetricsListener(registry);
    }

    @Test
    void incrementsValidRecordsForValidSink() {
        when(validProgressEvent.progress()).thenReturn(validProgress);
        when(validProgress.name()).thenReturn(LakehouseMetricsListener.VALID_SINK_NAME);
        when(validProgress.numInputRows()).thenReturn(42L);
        when(validProgress.batchDuration()).thenReturn(300L);

        listener.onQueryProgress(validProgressEvent);

        Counter valid = registry.find("lakehouse.records.valid").counter();
        assertThat(valid).isNotNull();
        assertThat(valid.count()).isEqualTo(42.0);
    }

    @Test
    void incrementsDeadLetterRecordsForDeadLetterSink() {
        when(deadLetterProgressEvent.progress()).thenReturn(deadLetterProgress);
        when(deadLetterProgress.name()).thenReturn(LakehouseMetricsListener.DEAD_LETTER_SINK_NAME);
        when(deadLetterProgress.numInputRows()).thenReturn(7L);
        when(deadLetterProgress.batchDuration()).thenReturn(0L);

        listener.onQueryProgress(deadLetterProgressEvent);

        Counter dl = registry.find("lakehouse.records.dead_letter").counter();
        assertThat(dl).isNotNull();
        assertThat(dl.count()).isEqualTo(7.0);
    }

    @Test
    void incrementsBatchCounterOnValidSinkProgress() {
        when(validProgressEvent.progress()).thenReturn(validProgress);
        when(validProgress.name()).thenReturn(LakehouseMetricsListener.VALID_SINK_NAME);
        when(validProgress.numInputRows()).thenReturn(10L);
        when(validProgress.batchDuration()).thenReturn(100L);

        listener.onQueryProgress(validProgressEvent);
        listener.onQueryProgress(validProgressEvent);

        Counter batches = registry.find("lakehouse.batches.total").counter();
        assertThat(batches).isNotNull();
        assertThat(batches.count()).isEqualTo(2.0);
    }

    @Test
    void ignoresUnknownSinkNames() {
        QueryProgressEvent unknownEvent = org.mockito.Mockito.mock(QueryProgressEvent.class);
        StreamingQueryProgress unknownProgress = org.mockito.Mockito.mock(StreamingQueryProgress.class);
        when(unknownEvent.progress()).thenReturn(unknownProgress);
        when(unknownProgress.name()).thenReturn("some-other-sink");

        listener.onQueryProgress(unknownEvent);

        // counters exist but remain at zero (registered at construction)
        assertThat(registry.find("lakehouse.records.valid").counter().count()).isEqualTo(0.0);
        assertThat(registry.find("lakehouse.records.dead_letter").counter().count()).isEqualTo(0.0);
    }
}
