package com.example.lakehouse.ingestion.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaSourceOptionsTest {

    private static final KafkaSourceConfig CONFIG = new KafkaSourceConfig(
            "broker:9092", "events", "test-group", "latest", 50_000L);

    @Mock
    private SparkSession mockSpark;

    @Mock
    private DataStreamReader mockReader;

    @Mock
    @SuppressWarnings("unchecked")
    private Dataset<Row> mockDataset;

    @BeforeEach
    void setUp() {
        when(mockSpark.readStream()).thenReturn(mockReader);
        when(mockReader.format(anyString())).thenReturn(mockReader);
        when(mockReader.option(anyString(), anyString())).thenReturn(mockReader);
        when(mockReader.option(anyString(), anyLong())).thenReturn(mockReader);
        when(mockReader.option(anyString(), anyBoolean())).thenReturn(mockReader);
        when(mockReader.load()).thenReturn(mockDataset);
    }

    @Test
    void passesAllSixOptionsToReader() {
        new KafkaSource(mockSpark, CONFIG).readStream();

        verify(mockReader).option("kafka.bootstrap.servers", "broker:9092");
        verify(mockReader).option("subscribe", "events");
        verify(mockReader).option("kafka.group.id", "test-group");
        verify(mockReader).option("startingOffsets", "latest");
        verify(mockReader).option("maxOffsetsPerTrigger", 50_000L);
        verify(mockReader).option("failOnDataLoss", false);
    }
}
