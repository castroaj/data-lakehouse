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

This is a **standalone Spark Structured Streaming job** (not Spring Boot). The pipeline is a single linear chain:

```
ConfigLoader → IngestionConfig
SparkSessionFactory → SparkSession
KafkaSource.readStream() → raw DataFrame
JsonTransformer.transform() → TransformResult { valid, deadLetter }
DeltaSink.write() → two concurrent writeStream() queries
```

`IngestionJob.main()` owns the wiring. `query.awaitTermination()` blocks until the job is stopped or crashes. A JVM shutdown hook calls `query.stop()` for clean termination.

### Classpath split — critical to understand

Spark is `compileOnly` (provided by the cluster at runtime). `delta-spark` is `implementation` (bundled). Delta Lake's own code lives partly in the `org.apache.spark.sql.delta.*` namespace — those ~2000 classes in the shadow JAR are Delta's code, not Spark core.

`testImplementation` extends `compileOnly` so unit tests can instantiate `SparkSession` in `local[1]` mode without a cluster.

The `aws-java-sdk-bundle` and `hadoop-aws` versions are pinned to exactly what Spark 3.5.8's Hadoop 3.3.4 was compiled against. **Do not bump either independently** — version skew causes `NoSuchMethodError` at runtime.

### Configuration

All runtime config comes from environment variables (no config files deployed with the JAR). `ConfigLoader` reads env vars → builds the immutable `IngestionConfig` record → throws `ConfigurationException` on any validation failure before Spark starts.

`DELTA_TABLE_PATH`, `CHECKPOINT_PATH`, and `DEAD_LETTER_PATH` are path segments relative to `S3_BUCKET_NAME`; the application prepends `s3a://<bucket>/`.

### Schema

Schemas are statically typed Java `StructType` definitions in `SchemaRegistry`, keyed by Kafka topic name. This is intentional — the compiler validates field types at build time. Adding a new topic means adding a new `StructType` entry in `SchemaRegistry` and rebuilding the JAR.

`JsonTransformer` uses `from_json()` with `columnNameOfCorruptRecord = "_corrupt_record"` to split rows: valid rows go to the Delta table, malformed rows go to a dead-letter JSON path on S3.

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
| `src/main/java/com/example/lakehouse/IngestionJob.java` | `main()` entry point |
| `src/main/java/com/example/lakehouse/config/IngestionConfig.java` | Immutable config record (all 13 fields) |
| `src/main/java/com/example/lakehouse/transform/SchemaRegistry.java` | Topic → StructType mapping |
