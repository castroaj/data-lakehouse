package com.example.lakehouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.lakehouse.config.ConfigLoader;
import com.example.lakehouse.config.IngestionConfig;
import com.example.lakehouse.exception.ConfigurationException;

public class IngestionJob {

    private static final Logger log = LoggerFactory.getLogger(IngestionJob.class);

    public static void main(String[] args) {
        IngestionConfig config;
        try {
            config = new ConfigLoader().load();
        } catch (ConfigurationException e) {
            log.error("Configuration validation failed: {}", e.getMessage());
            System.exit(1);
            return; // unreachable; satisfies definite-assignment since the compiler doesn't know System.exit never returns
        }

        // TODO: wire ConfigLoader → SparkSessionFactory → KafkaSource → JsonTransformer
        // → DeltaSink
        log.info("app={} topic={} master={}", config.sparkAppName(), config.kafkaTopic(), config.sparkMaster());
    }
}
