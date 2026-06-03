# Data Lakehouse Ingestion Service

A standalone Spark Structured Streaming job that continuously ingests JSON events from Apache Kafka into a Delta Lake table on AWS S3. Malformed records are quarantined to a dead-letter path rather than dropped, and the pipeline resumes exactly where it left off on restart.

---

## Purpose

Most data teams need a reliable, low-maintenance bridge between their event bus and their analytical store. This service fills that role: it runs as a single fat JAR, requires no Spring container or external coordinator, and leverages Spark's native streaming guarantees to deliver exactly-once writes to Delta Lake.

---

## How It Works

```
Kafka Topic (JSON)
       │
       ▼
  KafkaSource          — reads raw bytes via Spark Structured Streaming
       │
       ▼
  JsonTransformer      — parses against a statically typed StructType schema
       │                 valid rows continue; malformed rows are tagged
       ├──────────────► DeltaSink (valid)       → s3a://<bucket>/lakehouse/<table>/
       └──────────────► DeltaSink (dead-letter) → s3a://<bucket>/dead-letter/<table>/
```

All configuration comes from environment variables. The job validates every required value at startup and throws a `ConfigurationException` before allocating any Spark resources if anything is missing or invalid.

---

## Use Cases

- **Stream-to-lakehouse ingestion** — continuously land Kafka events into a queryable, ACID-compliant Delta table without a broker in between (no Flink, no Kafka Connect, no managed service required).
- **Schema-enforced ingestion** — schemas are statically declared in Java source (`SchemaRegistry`), giving compile-time validation and eliminating a runtime deserialization layer.
- **Fault-tolerant replay** — S3-backed checkpoints let the job resume from its last committed Kafka offset after any restart, with no manual offset management.
- **Malformed record handling** — bad messages are written to a separate dead-letter path in raw JSON form for inspection and reprocessing rather than silently discarded.

---

## Configuration

Set the following environment variables before running:

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | yes | — | Kafka broker list (`host:port,...`) |
| `KAFKA_TOPIC` | yes | — | Topic to consume |
| `KAFKA_GROUP_ID` | yes | — | Consumer group ID |
| `KAFKA_STARTING_OFFSETS` | no | `latest` | `earliest` or `latest` |
| `KAFKA_MAX_OFFSETS_PER_TRIGGER` | no | `100000` | Micro-batch size cap |
| `S3_BUCKET_NAME` | yes | — | Target S3 bucket |
| `AWS_REGION` | yes | — | AWS region |
| `DELTA_TABLE_PATH` | yes | — | Path relative to bucket root |
| `CHECKPOINT_PATH` | yes | — | Path relative to bucket root |
| `DEAD_LETTER_PATH` | yes | — | Path relative to bucket root |
| `TRIGGER_INTERVAL_SECONDS` | no | `30` | Micro-batch frequency |
| `SPARK_MASTER` | no | `local[*]` | Spark master URL |
| `SPARK_APP_NAME` | no | `lakehouse-ingestion` | Spark application name |

AWS credentials are resolved via the [Default Credential Provider Chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) — env vars, instance profile, ECS task role, etc.

A `.env` file with local defaults is included for development:

```bash
source .env && ./run.sh
```

---

## Building and Running

```bash
# Build fat JAR
./gradlew shadowJar
# → build/libs/data-lakehouse-ingestion-<version>-all.jar

# Run unit tests
./gradlew test

# Run integration tests (requires Docker)
./gradlew integrationTest

# Submit to a local Spark cluster
spark-submit \
  --class com.example.lakehouse.IngestionJob \
  --master local[4] \
  build/libs/data-lakehouse-ingestion-*-all.jar
```

---

## Project Layout

```
src/main/java/com/example/lakehouse/
├── IngestionJob.java           — main() entry point; wires all components
├── config/
│   ├── IngestionConfig.java    — immutable config record (13 fields)
│   └── ConfigLoader.java       — reads env vars; validates with Jakarta Bean Validation
├── spark/
│   └── SparkSessionFactory.java — builds SparkSession with Delta + S3A extensions
├── ingestion/
│   └── KafkaSource.java        — readStream() wrapper
├── transform/
│   ├── JsonTransformer.java    — parse JSON, split valid/dead-letter rows
│   └── SchemaRegistry.java     — topic → StructType mapping
├── sink/
│   └── DeltaSink.java          — writeStream() to Delta table and dead-letter path
└── exception/
    ├── LakehouseException.java
    └── ConfigurationException.java
```

---

## Technology

| Concern | Choice |
|---|---|
| Streaming engine | Spark Structured Streaming 3.5.8 (Scala 2.12) |
| Table format | Delta Lake 3.3.0 |
| Object storage | AWS S3 via `hadoop-aws` 3.3.4 + S3A |
| Build | Gradle 9.5 (Kotlin DSL) + Shadow plugin (fat JAR) |
| Config validation | Jakarta Bean Validation 3 / Hibernate Validator 8 |
| Unit tests | JUnit 5 + AssertJ + Mockito (local `SparkSession`) |
| Integration tests | Testcontainers 2 (Kafka) + S3Mock (Adobe) |

Spark JARs are `compileOnly` — the cluster provides them at runtime. Only Delta Lake, S3A, and logging are bundled in the fat JAR.

---

## Implementation Status

| Component | Status |
|---|---|
| `ConfigLoader` + `IngestionConfig` | Complete |
| Exception hierarchy | Complete |
| `SparkSessionFactory` | Stubbed |
| `KafkaSource` | Stubbed |
| `JsonTransformer` | Stubbed |
| `DeltaSink` | Stubbed |
| Unit tests — `ConfigLoaderTest` | Complete |
| Unit tests — `JsonTransformerTest` | Stubbed |
| Integration test — `IngestionJobIT` | Stubbed |

See [`SPEC.md`](SPEC.md) for the full design specification and component contracts.
