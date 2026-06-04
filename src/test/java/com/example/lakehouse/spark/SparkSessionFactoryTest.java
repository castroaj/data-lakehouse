package com.example.lakehouse.spark;

import org.apache.spark.sql.SparkSession;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.example.lakehouse.config.IngestionConfig;
import com.example.lakehouse.ingestion.kafka.KafkaSourceConfig;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkSessionFactoryTest {

    private static final IngestionConfig CONFIG = new IngestionConfig(
            new KafkaSourceConfig("broker:9092",
                    "events",
                    "group",
                    "latest",
                    100_000L),
            "my-bucket",
            "us-east-1",
            "lakehouse/events",
            "checkpoints/events",
            "dead-letter/events",
            30,
            "local[1]",
            "test-app");

    private SparkSession spark;

    @BeforeAll
    void createSession() {
        spark = SparkSessionFactory.createDeltaSparkSession(CONFIG);
    }

    @AfterAll
    void closeSession() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Nested
    class SparkDefaults {

        @Test
        void appNameMatchesConfig() {
            assertThat(spark.conf().get("spark.app.name")).isEqualTo(CONFIG.sparkAppName());
        }

        @Test
        void masterMatchesConfig() {
            assertThat(spark.conf().get("spark.master")).isEqualTo(CONFIG.sparkMaster());
        }
    }

    @Nested
    class DeltaLakeConfig {

        @Test
        void deltaExtensionRegistered() {
            assertThat(spark.conf().get("spark.sql.extensions"))
                    .isEqualTo("io.delta.sql.DeltaSparkSessionExtension");
        }

        @Test
        void deltaCatalogRegistered() {
            assertThat(spark.conf().get("spark.sql.catalog.spark_catalog"))
                    .isEqualTo("org.apache.spark.sql.delta.catalog.DeltaCatalog");
        }
    }

    @Nested
    class S3aConfig {

        @Test
        void s3aFilesystemImpl() {
            assertThat(spark.sparkContext().hadoopConfiguration().get("fs.s3a.impl"))
                    .isEqualTo("org.apache.hadoop.fs.s3a.S3AFileSystem");
        }

        @Test
        void s3aCredentialsProvider() {
            assertThat(spark.sparkContext().hadoopConfiguration().get("fs.s3a.aws.credentials.provider"))
                    .isEqualTo("com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        }

        @Test
        void s3aPathStyleAccessEnabled() {
            assertThat(spark.sparkContext().hadoopConfiguration().get("fs.s3a.path.style.access"))
                    .isEqualTo("true");
        }
    }
}
