package com.example.lakehouse.spark;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.lakehouse.config.IngestionConfig;

/**
 * Factory for creating SparkSession instances configured for our Lakehouse
 * ingestion job
 * 
 * @author Alexander Castro
 */
public class SparkSessionFactory {

    private static final Logger log = LoggerFactory.getLogger(SparkSessionFactory.class);

    /**
     * Private constructor for utility class - prevents instantiation
     */
    private SparkSessionFactory() {
    }

    /**
     * Creates and configures a SparkSession based on the provided IngestionConfig
     * 
     * @param config
     * @return
     */
    public static SparkSession createDeltaSparkSession(IngestionConfig config) {
        SparkSession spark = SparkSession.builder()
                .master(config.sparkMaster())
                .appName(config.sparkAppName())
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .getOrCreate();

        log.info("SPARK: app={} master={}",
                config.sparkAppName(),
                config.sparkMaster());
        return spark;
    }
}
