# Data Lakehouse Ingestion Service — Specification

## 1. Overview

A standalone Java 17 Spark application that continuously consumes JSON messages from an Apache Kafka topic using Spark Structured Streaming, applies a statically typed schema defined in Java source, and writes the results to a Delta Lake table on AWS S3. The application is packaged as a fat JAR and submitted to a Spark cluster (local mode for dev, standalone/YARN/EKS for prod).

This is not a Spring Boot application. Spark Structured Streaming is itself a long-running managed process; adding a Spring container would introduce classloader conflicts and unnecessary overhead. Lifecycle, configuration, and observability are handled through Spark's native mechanisms and structured logging.

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Apache Kafka                                                        │
│  Topic: <configured>   Partition: N   Format: JSON UTF-8            │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  Spark Kafka Source (structured streaming)
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│  IngestionJob  (main entry point)                                    │
│                                                                      │
│   SparkSessionFactory                                                │
│       └─ SparkSession  (Delta Lake + S3 extensions configured)       │
│                                                                      │
│   KafkaSource                                                        │
│       └─ readStream() → raw DataFrame (key, value, metadata cols)   │
│                                                                      │
│   JsonTransformer                                                    │
│       └─ parse value bytes → typed DataFrame using StructType schema │
│       └─ quarantine malformed rows → dead-letter path               │
│                                                                      │
│   DeltaSink                                                          │
│       └─ writeStream() → Delta Lake table on S3                     │
│       └─ checkpoint location on S3 (exactly-once semantics)         │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  S3A (hadoop-aws)
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│  AWS S3                                                              │
│  s3a://<bucket>/lakehouse/<table>/          ← Delta table (Parquet)  │
│  s3a://<bucket>/checkpoints/<table>/        ← Streaming checkpoint   │
│  s3a://<bucket>/dead-letter/<table>/        ← Malformed records      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. Technology Stack

| Component               | Library / Version                                        | Scope        |
|-------------------------|----------------------------------------------------------|--------------|
| JVM                     | Java 17 (Amazon Corretto 17 or Eclipse Temurin 17)       | runtime      |
| Build tool              | Gradle 9.5.0 (Kotlin DSL)                                | build        |
| Spark core + SQL        | `org.apache.spark:spark-sql_2.12:3.5.8`                  | provided     |
| Spark Kafka connector   | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`       | runtime      |
| Delta Lake              | `io.delta:delta-spark_2.12:3.3.0`                        | runtime      |
| Hadoop AWS (S3A)        | `org.apache.hadoop:hadoop-aws:3.3.4`                     | runtime      |
| AWS SDK (bundle)        | `com.amazonaws:aws-java-sdk-bundle:1.12.262`             | runtime      |
| Logging API             | `org.slf4j:slf4j-api:2.0.17`                             | runtime      |
| Logging impl            | `ch.qos.logback:logback-classic:1.5.23`                  | runtime      |
| JUnit 5                 | `org.junit.jupiter:junit-jupiter:5.13.4`                 | test         |
| AssertJ                 | `org.assertj:assertj-core:3.27.3`                        | test         |
| Mockito                 | `org.mockito:mockito-core:5.23.0`                        | test         |
| Testcontainers (Kafka)  | `org.testcontainers:kafka:2.0.5`                         | integration  |
| S3Mock (S3 stub)        | `com.adobe.testing:s3mock-testcontainers:3.11.0`         | integration  |

**Scala binary version**: `2.12` — used consistently across all Spark artifacts. Do not mix `2.12` and `2.13` artifacts.

**Spark scope**: Spark JARs are marked `compileOnly` / `provided` because the Spark runtime is provided by the cluster. The fat JAR must not re-bundle Spark.

**Why `hadoop-aws:3.3.4` and `aws-java-sdk-bundle:1.12.262`**: Spark 3.5.8 is built against Hadoop 3.3.4 (confirmed in its POM). Using a different hadoop-aws minor version risks S3A classpath conflicts at runtime. `1.12.262` is the exact AWS SDK V1 bundle version declared by the Hadoop 3.3.4 parent POM; bumping this independently risks `NoSuchMethodError` at runtime. Do not upgrade either of these in isolation.

**Why not LocalStack**: The LocalStack community edition (free, no-auth) was sunset in March 2026. Starting 2026-03-23, the `localstack/localstack` Docker image requires a `LOCALSTACK_AUTH_TOKEN` to start, making it unsuitable for open CI pipelines. `adobe/s3mock` is used instead — it is a purpose-built, free, zero-auth S3 stub with first-class Java + Testcontainers support.

**Why not MinIO**: MinIO entered maintenance mode in December 2025 and was archived in February 2026.

---

## 4. Project Layout

```
data-lakehouse/
├── build.gradle.kts
├── settings.gradle.kts
├── gradle/
│   └── wrapper/
│       └── gradle-wrapper.properties
├── SPEC.md
└── src/
    ├── main/
    │   ├── java/
    │   │   └── com/example/lakehouse/
    │   │       ├── IngestionJob.java               ← main() entry point
    │   │       ├── config/
    │   │       │   ├── IngestionConfig.java         ← immutable config record
    │   │       │   └── ConfigLoader.java            ← loads from env / sys props
    │   │       ├── spark/
    │   │       │   └── SparkSessionFactory.java     ← builds SparkSession
    │   │       ├── ingestion/
    │   │       │   └── KafkaSource.java             ← readStream() wrapper
    │   │       ├── transform/
    │   │       │   ├── JsonTransformer.java         ← parse + validate JSON
    │   │       │   └── SchemaRegistry.java          ← topic → StructType mapping
    │   │       ├── sink/
    │   │       │   └── DeltaSink.java               ← writeStream() to Delta
    │   │       └── util/
    │   │           └── SchemaUtil.java              ← shared StructType helpers
    │   └── resources/
    │       └── logback.xml
    └── test/
        ├── java/
        │   └── com/example/lakehouse/
        │       ├── transform/
        │       │   └── JsonTransformerTest.java     ← unit: schema parsing
        │       ├── config/
        │       │   └── ConfigLoaderTest.java        ← unit: env var loading
        │       └── integration/
        │           └── IngestionJobIT.java          ← IT: Kafka + LocalStack S3
        └── resources/
            └── logback-test.xml
```

---

## 5. Build Configuration (`build.gradle.kts`)

### Key concerns

1. **Fat JAR** — Use the `com.github.johnrengelman.shadow` plugin to produce a deployable uber-JAR. Spark and Hadoop JARs must be excluded from the shadow output.
2. **Scala suffix** — All Spark-ecosystem artifacts must use the `_2.12` suffix; the build file must enforce this via a version catalog or explicit coordinates.
3. **`provided` scope** — Gradle has no native `provided` scope; use `compileOnly` for Spark JARs and add them back to the `testImplementation` classpath so unit tests can instantiate `SparkSession`.
4. **Test split** — Unit tests run with `./gradlew test`; integration tests (classes ending in `IT`) run via a separate `integrationTest` source set and task, gated to only run when Docker is available.

### Dependency groups (logical)

```
// Provided by Spark cluster — compileOnly + testImplementation
spark-sql_2.12, spark-sql-kafka-0-10_2.12

// Bundled in fat JAR — implementation
delta-spark_2.12, hadoop-aws, aws-java-sdk-bundle, logback-classic

// Test only
junit-jupiter, assertj-core, mockito-core, testcontainers-kafka, s3mock-testcontainers
```

### Shadow JAR exclusions

The following must be excluded from the fat JAR to avoid conflicts with the cluster:
- `META-INF/*.SF`, `META-INF/*.DSA`, `META-INF/*.RSA` (signed JAR entries)
- `org/apache/spark/**`
- `org/apache/hadoop/security/**` (keep hadoop-aws S3A; exclude core hadoop if already provided)

---

## 6. Configuration Model

All configuration is read at startup from **environment variables** (preferred for container/k8s deployments) with fallback to Java system properties. There are no config files to deploy alongside the JAR.

### `IngestionConfig` record

```
Field                       Env var                         Required  Default
─────────────────────────────────────────────────────────────────────────────
kafkaBootstrapServers       KAFKA_BOOTSTRAP_SERVERS         yes       —
kafkaTopic                  KAFKA_TOPIC                     yes       —
kafkaGroupId                KAFKA_GROUP_ID                  yes       —
kafkaStartingOffsets        KAFKA_STARTING_OFFSETS          no        "latest"
kafkaMaxOffsetsPerTrigger   KAFKA_MAX_OFFSETS_PER_TRIGGER   no        100000
s3BucketName                S3_BUCKET_NAME                  yes       —
s3Region                    AWS_REGION                      yes       —
deltaTablePath              DELTA_TABLE_PATH                yes       —  (relative to bucket root)
checkpointPath              CHECKPOINT_PATH                 yes       —  (relative to bucket root)
deadLetterPath              DEAD_LETTER_PATH                yes       —  (relative to bucket root)
triggerIntervalSeconds      TRIGGER_INTERVAL_SECONDS        no        30
sparkMaster                 SPARK_MASTER                    no        "local[*]"
sparkAppName                SPARK_APP_NAME                  no        "lakehouse-ingestion"
```

`DELTA_TABLE_PATH`, `CHECKPOINT_PATH`, and `DEAD_LETTER_PATH` are path segments; the application constructs full `s3a://` URIs by prepending `s3a://<S3_BUCKET_NAME>/`.

AWS credentials are resolved by the AWS Default Credential Provider Chain (env vars, instance profile, ECS task role, etc.) — never hardcoded.

### Validation rules

- `kafkaBootstrapServers` must be non-blank
- `kafkaTopic` must be non-blank
- `kafkaGroupId` must be non-blank
- `s3BucketName` must be non-blank
- `deltaTablePath`, `checkpointPath`, `deadLetterPath` must be non-blank and must not start with `s3a://` (they are relative segments)
- `triggerIntervalSeconds` must be ≥ 1
- `kafkaMaxOffsetsPerTrigger` must be ≥ 1

Violations throw `ConfigurationException` (unchecked) at startup before any Spark resource is allocated.

---

## 7. Core Components

### 7.1 `IngestionJob` (main entry point)

```
static void main(String[] args)
  1. Load IngestionConfig via ConfigLoader
  2. Create SparkSession via SparkSessionFactory
  3. Resolve schema via SchemaRegistry (from topic name)
  4. Build streaming query:
       KafkaSource.readStream()
     → JsonTransformer.transform()
     → DeltaSink.writeStream()
  5. query.awaitTermination()
```

Registers a JVM shutdown hook that calls `query.stop()` cleanly.

### 7.2 `SparkSessionFactory`

Constructs a `SparkSession` with the following configuration applied:
- `spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension`
- `spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog`
- `fs.s3a.impl = org.apache.hadoop.fs.s3a.S3AFileSystem`
- `fs.s3a.aws.credentials.provider = com.amazonaws.auth.DefaultAWSCredentialsProviderChain`
- `fs.s3a.path.style.access = true` (required for S3Mock in tests)
- `spark.master` from config (allows override to `local[*]` for dev)

### 7.3 `KafkaSource`

Wraps `spark.readStream().format("kafka")` with options derived from `IngestionConfig`. Returns a `Dataset<Row>` containing the raw Kafka columns:
- `key` (binary)
- `value` (binary) ← primary payload
- `topic`, `partition`, `offset`, `timestamp`, `timestampType` ← metadata kept for audit

No schema parsing is done here — this component is only responsible for the Kafka connection.

### 7.4 `SchemaRegistry`

A pure static mapping from Kafka topic name to `StructType`, implemented entirely in Java source. Schemas are expressed as typed `StructType` / `StructField` constructions — no deserialization layer, no external config file. Adding a new schema means adding a new entry in Java code and rebuilding the JAR; this is intentional: the compiler validates field types and nullability at build time.

For the initial implementation a single schema is registered. The component returns `Optional<StructType>` and throws `SchemaNotFoundException` (unchecked) if no schema is registered for the given topic.

**Future aspiration**: loading schemas from a local file (e.g., a JSON schema file mounted as a Kubernetes ConfigMap) would decouple schema authorship from the build cycle without adding a remote runtime dependency. This is not in scope for v1; the static approach is chosen to eliminate a deserialization layer and keep compile-time safety.

Schema fields include an `_ingested_at` timestamp and `_kafka_offset` long automatically appended by the transformer.

### 7.5 `JsonTransformer`

Takes the raw Kafka `Dataset<Row>` and a `StructType`, returns two datasets: `valid` and `dead_letter`.

```
transform(Dataset<Row> raw, StructType schema) → TransformResult

TransformResult record:
  Dataset<Row> valid         ← rows matching schema
  Dataset<Row> deadLetter    ← rows that failed parsing (raw value + error message)
```

Implementation uses `from_json(col("value").cast("string"), schema)` with `columnNameOfCorruptRecord` set to `_corrupt_record`. Rows where `_corrupt_record` is non-null are routed to dead letter.

Appends metadata columns:
- `_ingested_at` = `current_timestamp()`
- `_kafka_offset` = `col("offset")`
- `_kafka_partition` = `col("partition")`

### 7.6 `DeltaSink`

Writes valid records to the Delta table and dead-letter records to a separate path. Each write is a separate `writeStream()` query.

**Valid records**:
```
dataset.writeStream()
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpointPath)
  .trigger(Trigger.ProcessingTime(intervalSeconds + " seconds"))
  .start(deltaTablePath)
```

**Dead-letter records**:
```
dataset.writeStream()
  .format("json")          ← plain JSON, not Delta, for easy inspection
  .outputMode("append")
  .option("checkpointLocation", deadLetterCheckpointPath)
  .trigger(Trigger.ProcessingTime(intervalSeconds + " seconds"))
  .start(deadLetterPath)
```

---

## 8. Data Flow (end-to-end)

```
Kafka message (JSON bytes)
  │
  ▼
KafkaSource.readStream()
  → DataFrame: [key, value(bytes), topic, partition, offset, timestamp]
  │
  ▼
JsonTransformer.transform(rawDf, schema)
  → cast value bytes to String
  → from_json() with StructType
  → split: valid rows / rows with _corrupt_record != null
  → append _ingested_at, _kafka_offset, _kafka_partition
  │
  ├─ valid rows ──────────────────────────────────────────▶ DeltaSink → S3 Delta table
  │
  └─ malformed rows ─────────────────────────────────────▶ DeltaSink dead-letter → S3 JSON
```

---

## 9. Schema Example

The initial target schema (topic: `events`) — stored as a `StructType` in `SchemaRegistry`:

```
eventId       STRING    NOT NULL
eventType     STRING    NOT NULL
occurredAt    TIMESTAMP NOT NULL
sourceSystem  STRING    NOT NULL
payload       STRING             ← raw JSON sub-document kept as string
```

Appended by transformer:
```
_ingested_at    TIMESTAMP
_kafka_offset   LONG
_kafka_partition INT
```

Schema evolution is handled via Delta Lake's `mergeSchema` option (off by default; can be enabled via `DELTA_MERGE_SCHEMA=true` env var mapped to `spark.databricks.delta.schema.autoMerge.enabled`).

---

## 10. Error Handling

| Failure scenario                        | Behavior                                                                 |
|-----------------------------------------|--------------------------------------------------------------------------|
| Missing required env var                | `ConfigurationException` thrown in `main()` before Spark starts         |
| No schema registered for topic          | `SchemaNotFoundException` thrown during startup wiring                   |
| Malformed JSON in Kafka message         | Row routed to dead-letter path; stream continues                         |
| Transient S3 write failure              | Spark retries per `spark.task.maxFailures` (default: 4); then stream task fails |
| Fatal stream query exception            | JVM exits with non-zero code; orchestrator (k8s/YARN) restarts          |
| Kafka broker unreachable at startup     | Spark Kafka source retries per `kafkaConsumer.pollTimeoutMs` config      |

Application-level exceptions hierarchy:
```
RuntimeException
  └── LakehouseException               ← base unchecked domain exception
        ├── ConfigurationException     ← invalid/missing config
        ├── SchemaNotFoundException    ← no schema for topic
        └── IngestionException         ← wraps unexpected runtime errors
```

---

## 11. Checkpointing and Exactly-Once Semantics

Spark Structured Streaming with Delta Lake sink provides **exactly-once** end-to-end guarantees when:
1. The checkpoint directory is on a durable, consistent store (S3 with strong consistency — guaranteed since Dec 2020).
2. The Delta table uses its default transactional commit protocol.
3. `outputMode("append")` is used (no aggregations that require reprocessing).

Checkpoint path layout on S3:
```
s3a://<bucket>/checkpoints/<table>/valid/      ← valid records query checkpoint
s3a://<bucket>/checkpoints/<table>/dead-letter/ ← dead-letter query checkpoint
```

On restart, the streaming query resumes from the last committed offset automatically — no manual offset management required.

---

## 12. Observability

### Logging

All classes use:
```java
private static final Logger log = LoggerFactory.getLogger(MyClass.class);
```

Log format: structured key=value lines (not JSON) to keep compatibility with CloudWatch Logs Insights and simple grep.

Key log events:

| Event                          | Level | Fields                                          |
|--------------------------------|-------|-------------------------------------------------|
| Job started                    | INFO  | `app=, topic=, table=, master=`                 |
| Batch committed                | INFO  | `batch_id=, rows_written=, duration_ms=`        |
| Dead-letter records written    | WARN  | `batch_id=, dead_letter_count=`                 |
| Schema not found               | ERROR | `topic=`                                        |
| Config validation failure      | ERROR | `field=, reason=`                               |
| Stream query terminated        | INFO  | `reason=`                                       |

### Spark UI

Local dev exposes Spark UI on `http://localhost:4040`. In cluster mode, the cluster manager's history server is used.

### Metrics (future)

Spark's built-in `StreamingQueryListener` can be implemented to push per-batch metrics to a sink (CloudWatch, Prometheus pushgateway). Out of scope for v1 but the `DeltaSink` is designed to accept an optional `StreamingQueryListener` via constructor injection to avoid a future breaking change.

---

## 13. Testing Strategy

### Unit Tests (`./gradlew test`)

Target: pure logic with no external dependencies.

| Class under test    | Test class               | What is tested                                              |
|---------------------|--------------------------|-------------------------------------------------------------|
| `ConfigLoader`      | `ConfigLoaderTest`       | Required field validation, defaults, type coercion          |
| `JsonTransformer`   | `JsonTransformerTest`    | Valid parse, malformed row routing, metadata column appends |
| `SchemaRegistry`    | `SchemaRegistryTest`     | Known topic returns schema, unknown topic throws            |

`JsonTransformerTest` creates a local `SparkSession` in `local[1]` mode (fast; no cluster needed) using an in-memory DataFrame as input. This is standard practice for Spark unit tests.

### Integration Tests (`./gradlew integrationTest`)

Require Docker. Use Testcontainers.

| Test class           | Containers                       | What is tested                                                         |
|----------------------|----------------------------------|------------------------------------------------------------------------|
| `IngestionJobIT`     | `KafkaContainer`, `S3MockContainer` | Full end-to-end: produce messages → run job → assert Delta table on mock S3 |

`S3MockContainer` (`com.adobe.testing:s3mock-testcontainers`) exposes an S3-compatible HTTP endpoint with no auth required. `SparkSessionFactory` sets `fs.s3a.endpoint` to the S3Mock URL and `fs.s3a.path.style.access=true` for compatibility.

**Testcontainers 2.0 note**: Use `org.testcontainers.kafka.KafkaContainer` (Apache native image: `apache/kafka`) — the old `org.testcontainers.containers.KafkaContainer` class is deprecated in Testcontainers 2.x. For Confluent images use `org.testcontainers.kafka.ConfluentKafkaContainer`.

The integration test:
1. Starts `KafkaContainer` + `S3MockContainer` (static, shared across test methods)
2. Creates the S3 bucket via the AWS SDK V1 client pointed at the S3Mock endpoint
3. Produces N test messages to the Kafka topic
4. Starts `IngestionJob` in a background thread with a short trigger interval
5. Polls (with bounded retries) until the Delta table exists and row count matches
6. Asserts schema correctness and metadata column presence
7. Produces a malformed message and asserts it appears in the dead-letter path

---

## 14. Packaging and Deployment

### Fat JAR

Built via the Shadow plugin:
```
./gradlew shadowJar
→ build/libs/data-lakehouse-ingestion-<version>-all.jar
```

JAR manifest sets `Main-Class: com.example.lakehouse.IngestionJob`.

### Spark Submit (local dev)

```bash
spark-submit \
  --class com.example.lakehouse.IngestionJob \
  --master local[4] \
  build/libs/data-lakehouse-ingestion-<version>-all.jar
```

### Kubernetes (prod)

The JAR is bundled in a Docker image based on `apache/spark:3.5.8-java17`. The image is submitted via `spark-operator` or `spark-submit --master k8s://`.

Required environment variables are injected via Kubernetes `Secret` / `ConfigMap`.

---

## 15. Out of Scope for v1

- File-based schema loading (e.g., JSON schema file via ConfigMap) — schemas are statically typed in Java source for v1; file-based loading is the aspirational next step toward a reusable binary
- Schema Registry integration (Confluent / Avro / Protobuf) — JSON + static Java schema only for v1
- Delta table compaction / `OPTIMIZE` / `VACUUM` — run as a separate maintenance job
- Multi-topic fan-out — one topic → one table per job instance
- Authentication to Kafka (SASL/SSL) — add via `kafka.security.protocol` Spark option in v2
- REST management API — not needed; job is fire-and-forget with checkpoint-based recovery

---

## 16. Open Questions (to resolve before coding)

1. **Topic name** — What is the initial Kafka topic name and what does its JSON schema look like?
2. **S3 bucket naming convention** — Will dev/staging/prod use separate buckets or path prefixes?
3. **Delta table partitioning** — Should the table be partitioned (e.g., by date or `eventType`)? Partitioning affects write performance and query efficiency.
4. **Trigger interval** — 30 seconds default acceptable for the latency SLA?
5. **`kafkaStartingOffsets`** — For the first deployment, should the job start from `earliest` (replay all history) or `latest` (ignore backlog)?
