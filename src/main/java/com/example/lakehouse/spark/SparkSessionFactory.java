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
     * Creates and configures a SparkSession for Delta Lake ingestion from S3, using
     * the master URL and application name from the provided config.
     *
     * @param config the ingestion config supplying {@code sparkMaster} and
     *               {@code sparkAppName}; must not be null
     * @return a configured SparkSession with Delta Lake and S3A extensions registered
     */
    public static SparkSession createDeltaSparkSession(IngestionConfig config) {
        SparkSession spark = SparkSession.builder()
                .master(config.sparkMaster())
                .appName(config.sparkAppName())
                // Delta Lake SQL extensions and catalog override required to enable Delta tables
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // S3A filesystem and credential chain for reading/writing S3 paths
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                // Path-style access required for S3-compatible endpoints (e.g., LocalStack, MinIO)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .getOrCreate();

        log.info("SPARK: app={} master={}",
                config.sparkAppName(),
                config.sparkMaster());
        return spark;
    }
}
