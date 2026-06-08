# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build commands

```bash
./gradlew build              # compile + unit tests + shadow JAR
./gradlew test               # unit tests only
./gradlew shadowJar          # fat JAR → build/libs/*-all.jar
./gradlew compileJava        # compile without running tests
./gradlew integrationTest    # integration tests (requires Docker; task not yet wired)

# Run a single test class
./gradlew test --tests "com.example.lakehouse.transform.JsonTransformerTest"

# Run a single test method
./gradlew test --tests "com.example.lakehouse.transform.JsonTransformerTest.someMethod"
```

## Release versioning (nebula-release)

Version is inferred from git tags — `fetch-depth: 0` is required in any CI checkout step.

```bash
./gradlew devSnapshot   # 0.1.0-dev.N+<hash>  — local dev build
./gradlew snapshot      # 0.1.0-SNAPSHOT
./gradlew candidate     # 0.1.0-rc.1 + creates git tag
./gradlew final         # 0.1.0 + creates git tag
```

## Architecture

This is a **standalone Spark Structured Streaming job** (not Spring Boot). The pipeline is a fully wired linear chain:

```
ConfigLoader → IngestionConfig
SparkSessionFactory → SparkSession
KafkaSource.readStream() → raw DataFrame
JsonTransformer.transform() → TransformResult { valid, deadLetter }
DeltaSink.write() → two concurrent writeStream() queries
```

`IngestionJob.main()` owns the wiring. Both streaming queries run concurrently; `query.awaitTermination()` blocks until the job is stopped or crashes. A JVM shutdown hook calls `sink.close()` → `query.stop()` for each query on SIGTERM.

### Pipeline interface layer

All three pipeline stages follow the same interface pattern — single method, `AutoCloseable`:

- `Source` — `readStream() → Dataset<Row>`; top-level ingestion interface
- `Transformer` — `transform(Dataset<Row>) → TransformResult`; stateless by default (`close()` is a no-op)
- `Sink` — `write(TransformResult) → List<StreamingQuery>`; returns both active queries so the caller can `awaitTermination()`

Implementations: `KafkaSource implements IKafkaSource extends Source`, `JsonTransformer implements Transformer`, `DeltaSink implements Sink`.

### Source abstraction

- `Source` — top-level `AutoCloseable` interface with a single `readStream()` method
- `IKafkaSource extends Source` — adds `readBatch(KafkaOffsetRange)` for bounded reads (backfill / replay)
- `KafkaSource implements IKafkaSource` — concrete Kafka implementation; accepts either `IngestionConfig` or `KafkaSourceConfig` directly

Kafka-specific configuration is isolated in `KafkaSourceConfig` (a record with Bean Validation constraints), nested inside `IngestionConfig` via the `kafkaSourceConfig()` accessor.

### Classpath split — critical to understand

Spark is `compileOnly` (provided by the cluster at runtime). `delta-spark` is `implementation` (bundled). Delta Lake's own code lives partly in the `org.apache.spark.sql.delta.*` namespace — those ~2000 classes in the shadow JAR are Delta's code, not Spark core.

`testImplementation` extends `compileOnly` so unit tests can instantiate `SparkSession` in `local[1]` mode without a cluster.

The `aws-java-sdk-bundle` and `hadoop-aws` versions are pinned to exactly what Spark 3.5.8's Hadoop 3.3.4 was compiled against. **Do not bump either independently** — version skew causes `NoSuchMethodError` at runtime.

### Configuration

All runtime config comes from environment variables (no config files deployed with the JAR). `ConfigLoader` reads env vars → builds the immutable `IngestionConfig` record → throws `ConfigurationException` on any validation failure before Spark starts.

`IngestionConfig` has 10 fields: a nested `KafkaSourceConfig` (bootstrapServers, topic, groupId, startingOffsets, maxOffsetsPerTrigger), plus s3BucketName, s3Region, deltaTablePath, checkpointPath, deadLetterPath, triggerIntervalSeconds, sparkMaster, sparkAppName, and a nested `MetricsConfig` (metricsPort). Both records use Jakarta Bean Validation (`@NotBlank`, `@Pattern`, `@Min`, `@Valid`) enforced by `ConfigLoader`.

`DELTA_TABLE_PATH`, `CHECKPOINT_PATH`, and `DEAD_LETTER_PATH` are path segments relative to `S3_BUCKET_NAME`; the application prepends `s3a://<bucket>/`. Supplying an `s3a://` URI directly is rejected by a `@Pattern` constraint.

`METRICS_PORT` defaults to `9090`. Must be ≥ 1024.

### Schema

Schemas are defined as Avro `.avsc` files on the classpath under `src/main/resources/schemas/{topic}.avsc`. They are loaded and converted to Spark `StructType` at startup — a missing schema file causes immediate failure before any Kafka connection is made.

The loading chain is:

- `SchemaProvider` — interface: `schemaFor(topic) → StructType`; throws `SchemaNotFoundException` if absent
- `ClasspathAvroSchemaProvider implements SchemaProvider` — loads `/schemas/{topic}.avsc` from the classpath eagerly at construction; pass the full set of topics at startup for fail-fast validation
- `AvroSchemaConverter` — static utility: converts `org.apache.avro.Schema → StructType`; handles nullable unions (`["null","T"]` → nullable Spark field), nested records, arrays, and maps
- `SchemaRegistry` — thin facade over a `SchemaProvider`; provides a stable internal API

To add a new topic: add `{topic}.avsc` to `src/main/resources/schemas/`, pass the topic name when constructing `ClasspathAvroSchemaProvider`.

`JsonTransformer` uses `from_json()` with `columnNameOfCorruptRecord = "_corrupt_record"` to split rows: valid rows (corrupt column is null) go to the Delta table with `_ingested_at`, `_kafka_offset`, and `_kafka_partition` metadata columns appended; malformed rows go to a dead-letter JSON path on S3.

### Observability

`LakehouseMetricsListener extends StreamingQueryListener` fires after each Spark micro-batch and routes metrics by query name:

- `"lakehouse-valid-sink"` → increments `lakehouse.records.valid` and `lakehouse.batches.total`; records `lakehouse.batch.duration.ms`
- `"lakehouse-dead-letter-sink"` → increments `lakehouse.records.dead_letter`

All meters are registered against a `MeterRegistry` (Micrometer). `IngestionJob` wires a `PrometheusMeterRegistry`; swapping backends requires changing one line in `main()`.

`MetricsServer` exposes `/metrics` on `METRICS_PORT` (default 9090) via a JDK `HttpServer` — no extra HTTP framework dependency. Prometheus scrapes this endpoint. The server starts before Spark initialises so metrics are available from the first batch.

**Important:** row counts cannot be measured inside `JsonTransformer` because `Dataset<Row>` is a lazy plan. `dataset.count()` in a streaming context forces full materialisation. Use `StreamingQueryProgress.numInputRows()` from the listener instead.

### Testing

Unit tests (`*Test.java`) use a local `SparkSession` in `local[1]` mode — no cluster or Docker required.

Integration tests (`*IT.java`) use Testcontainers:
- Kafka: `org.testcontainers.kafka.KafkaContainer` (Apache native image — the old `org.testcontainers.containers.KafkaContainer` is deprecated in Testcontainers 2.x)
- S3: `com.adobe.testing:s3mock-testcontainers` — LocalStack requires a paid auth token since March 2026 and MinIO was archived in February 2026

### Shadow JAR notes

- `isZip64 = true` is required — `aws-java-sdk-bundle` alone exceeds 65 535 ZIP entries
- `mergeServiceFiles()` is set to correctly merge `META-INF/services/` entries (needed for S3A filesystem registration)
- Signed JAR entries (`*.SF`, `*.DSA`, `*.RSA`, `*.EC`) are stripped to prevent `SecurityException` at runtime

## Key files

| File | Purpose |
|---|---|
| `SPEC.md` | Full design spec — authoritative source for component contracts, config model, error handling, and testing strategy |
| `build.gradle.kts` | Single build file; all dependency versions are inlined here |
| `src/main/java/com/example/lakehouse/IngestionJob.java` | `main()` entry point; fully wired pipeline with ordered resource lifecycle and shutdown hook |
| `src/main/java/com/example/lakehouse/config/IngestionConfig.java` | Immutable config record (10 fields; Kafka settings in `KafkaSourceConfig`, metrics settings in `MetricsConfig`) |
| `src/main/java/com/example/lakehouse/config/MetricsConfig.java` | Metrics config record (`metricsPort`; loaded from `METRICS_PORT`) |
| `src/main/java/com/example/lakehouse/config/ConfigLoader.java` | Reads env vars, validates with Jakarta Bean Validation, throws `ConfigurationException` |
| `src/main/java/com/example/lakehouse/ingestion/Source.java` | Top-level source interface (`AutoCloseable` + `readStream()`) |
| `src/main/java/com/example/lakehouse/ingestion/kafka/IKafkaSource.java` | Kafka-specific source interface; adds `readBatch(KafkaOffsetRange)` |
| `src/main/java/com/example/lakehouse/ingestion/kafka/KafkaSource.java` | Concrete Kafka source implementing `IKafkaSource` |
| `src/main/java/com/example/lakehouse/ingestion/kafka/KafkaSourceConfig.java` | Kafka config record with Bean Validation constraints |
| `src/main/java/com/example/lakehouse/transform/Transformer.java` | Transformer interface (`AutoCloseable` + `transform(Dataset<Row>)`) |
| `src/main/java/com/example/lakehouse/transform/TransformResult.java` | Record holding `valid` and `deadLetter` output datasets from a transformer |
| `src/main/java/com/example/lakehouse/transform/JsonTransformer.java` | Parses Kafka JSON payloads; splits valid/dead-letter via `_corrupt_record`; appends metadata columns |
| `src/main/java/com/example/lakehouse/transform/SchemaRegistry.java` | Facade over `SchemaProvider`; stable internal API for topic → StructType lookup |
| `src/main/java/com/example/lakehouse/transform/schema/SchemaProvider.java` | Interface: `schemaFor(topic) → StructType` |
| `src/main/java/com/example/lakehouse/transform/schema/ClasspathAvroSchemaProvider.java` | Loads `.avsc` files from classpath eagerly at construction; fail-fast on missing schemas |
| `src/main/java/com/example/lakehouse/transform/schema/AvroSchemaConverter.java` | Static utility: `org.apache.avro.Schema → Spark StructType` |
| `src/main/java/com/example/lakehouse/sink/Sink.java` | Sink interface (`AutoCloseable` + `write(TransformResult)`) |
| `src/main/java/com/example/lakehouse/sink/DeltaSink.java` | Writes valid rows to Delta, dead-letter rows to JSON; registers `StreamingQueryListener` |
| `src/main/java/com/example/lakehouse/metrics/LakehouseMetricsListener.java` | `StreamingQueryListener` implementation; routes per-batch row counts to Micrometer |
| `src/main/java/com/example/lakehouse/metrics/MetricsServer.java` | JDK `HttpServer` exposing `/metrics` for Prometheus scraping |
| `src/main/java/com/example/lakehouse/exception/LakehouseException.java` | Abstract base exception for all application exceptions |
| `src/main/java/com/example/lakehouse/exception/ConfigurationException.java` | Thrown when config validation fails |
| `src/main/java/com/example/lakehouse/exception/SchemaNotFoundException.java` | Thrown when no `.avsc` schema is found for a topic |
| `src/main/resources/schemas/events.avsc` | Sample Avro schema for the `events` topic |
