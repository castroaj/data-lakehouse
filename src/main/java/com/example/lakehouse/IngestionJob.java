package com.example.lakehouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.lakehouse.config.ConfigLoader;
import com.example.lakehouse.config.IngestionConfig;
import com.example.lakehouse.exception.ConfigurationException;
import com.example.lakehouse.spark.SparkSessionFactory;

public class IngestionJob {

    private static final Logger log = LoggerFactory.getLogger(IngestionJob.class);

    public static void main(String[] args) {
        IngestionConfig config;
        try {
            config = new ConfigLoader().load();
        } catch (ConfigurationException e) {
            log.error("Configuration validation failed: {}", e.getMessage());
            System.exit(1);
            return;
        }

        // Construct the delta spark session
        var deltaSparkSession = SparkSessionFactory.createDeltaSparkSession(config);

        // TODO: wire ConfigLoader → SparkSessionFactory → KafkaSource → JsonTransformer
        // → DeltaSink
        log.info("app={} topic={} master={}", config.sparkAppName(), config.kafkaTopic(), config.sparkMaster());
    }
}
