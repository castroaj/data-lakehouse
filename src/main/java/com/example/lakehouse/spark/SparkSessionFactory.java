package com.example.lakehouse.spark;

import com.example.lakehouse.config.IngestionConfig;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    public SparkSession create(IngestionConfig config) {
        // TODO: configure Delta Lake extensions, S3A filesystem, and S3 credentials provider
        throw new UnsupportedOperationException("not implemented");
    }
}
