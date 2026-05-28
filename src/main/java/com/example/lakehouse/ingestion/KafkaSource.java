package com.example.lakehouse.ingestion;

import com.example.lakehouse.config.IngestionConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaSource {

    public Dataset<Row> readStream(SparkSession spark, IngestionConfig config) {
        // TODO: spark.readStream().format("kafka") with options from config
        throw new UnsupportedOperationException("not implemented");
    }
}
