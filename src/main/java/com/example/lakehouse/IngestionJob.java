package com.example.lakehouse;

import com.example.lakehouse.config.ConfigLoader;
import com.example.lakehouse.config.IngestionConfig;
import com.example.lakehouse.exception.ConfigurationException;
import com.example.lakehouse.ingestion.Source;
import com.example.lakehouse.ingestion.kafka.KafkaSource;
import com.example.lakehouse.metrics.LakehouseMetricsListener;
import com.example.lakehouse.metrics.MetricsServer;
import com.example.lakehouse.sink.DeltaSink;
import com.example.lakehouse.sink.Sink;
import com.example.lakehouse.spark.SparkSessionFactory;
import com.example.lakehouse.transform.JsonTransformer;
import com.example.lakehouse.transform.TransformResult;
import com.example.lakehouse.transform.Transformer;
import com.example.lakehouse.transform.schema.ClasspathAvroSchemaProvider;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Entry point for the Lakehouse ingestion job. Wires together
 * {@link ConfigLoader}, {@link SparkSessionFactory}, {@link KafkaSource},
 * {@link JsonTransformer}, and {@link DeltaSink} into a single linear
 * Structured Streaming pipeline.
 *
 * @author Alexander Castro
 */
public class IngestionJob {

    private static final Logger log = LoggerFactory.getLogger(IngestionJob.class);

    /**
     * Starts the ingestion pipeline. Exits with code 1 if configuration validation
     * fails or if an unhandled exception terminates the pipeline; blocks until all
     * streaming queries terminate normally.
     *
     * @param args command-line arguments; unused — all configuration comes from
     *             environment variables
     */
    public static void main(String[] args) {
        IngestionConfig config;
        try {
            config = new ConfigLoader().load();
        } catch (ConfigurationException e) {
            log.error("config_validation_failed error={}", e.getMessage());
            System.exit(1);
            return;
        }

        log.info("job_starting app={} topic={} master={}",
                config.sparkAppName(), config.kafkaSourceConfig().topic(), config.sparkMaster());

        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        try (MetricsServer metricsServer = new MetricsServer(registry, config.metricsConfig().metricsPort())) {
            metricsServer.start();

            SparkSession spark = SparkSessionFactory.createDeltaSparkSession(config);
            String topic = config.kafkaSourceConfig().topic();

            var schemaProvider = new ClasspathAvroSchemaProvider(List.of(topic));

            try (Source source = new KafkaSource(spark, config);
                 Transformer transformer = new JsonTransformer(schemaProvider, topic, registry);
                 Sink sink = new DeltaSink(spark, config, new LakehouseMetricsListener(registry))) {

                TransformResult result = transformer.transform(source.readStream());
                List<StreamingQuery> queries = sink.write(result);

                // clean shutdown on SIGTERM / SIGINT
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    log.info("shutdown_triggered");
                    sink.close();
                }));

                for (StreamingQuery q : queries) {
                    try {
                        q.awaitTermination();
                    } catch (StreamingQueryException e) {
                        log.error("query_error name={} error={}", q.name(), e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.error("job_failed error={}", e.getMessage(), e);
            System.exit(1);
        }
    }
}
