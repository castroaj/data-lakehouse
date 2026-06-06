---
name: java-doc-standards
description: "Java documentation standards: class Javadoc, method headers, field comments, validation annotations, and insightful inline comments."
---

# Java Documentation Standards

Consistent, meaningful documentation for all Java source files in this project.
Take inspiration from `KafkaSource.java` and `ConfigLoader.java` — the gold-standard examples in this codebase.

## When to Activate

- Writing any new Java class, interface, record, or enum
- Adding new public or package-private methods to an existing type
- Adding non-trivial private fields or static constants
- Invoked alongside `feature-dev` or `java-coding-standards`

---

## 1. Class-Level Javadoc

**Required on every public class, interface, record, and enum.**

```java
// PASS
/**
 * A Kafka source that supports both streaming and batch reads based on offset
 * ranges.
 *
 * @author Alexander Castro
 * @see IKafkaSource
 */
public class KafkaSource implements IKafkaSource {
```

```java
// FAIL — class has no documentation
public class DeltaSink implements Sink {
```

Rules:
- One to two sentence summary of the type's single responsibility
- `@author Alexander Castro` always present
- `@see RelatedType` only when the type implements a non-obvious interface or meaningfully delegates to another type

---

## 2. Method Javadoc

**Required on all public and package-private methods.**

```java
// PASS
/**
 * Parses the provided string as a long, returning the default value if the
 * string is null, and throwing a ConfigurationException if it is not a valid long.
 *
 * @param field        the name of the configuration field (used in error messages)
 * @param value        the string value to parse; may be null
 * @param defaultValue the value to return when value is null
 * @return the parsed long value, or defaultValue if value is null
 * @throws ConfigurationException if value is non-null but not a valid long
 */
private static long parseLong(String field, String value, long defaultValue) {
```

```java
// FAIL — no documentation on a non-trivial method
public TransformResult transform(Dataset<Row> raw) {
```

Rules:
- `@param` for every parameter; include constraints such as "must not be null" or "must be positive"
- `@return` for every non-void method; be specific about meaningful return values (never just "the result")
- `@throws` for every checked exception and for unchecked exceptions that are part of the documented contract
- Private methods: add Javadoc only when the purpose or behavior is non-obvious

---

## 3. Field-Level Documentation

**Required for non-trivial private fields and constants with non-obvious values.**

```java
// PASS
/**
 * The Spark session to use for reading from Kafka. This should be a long-lived
 * session that is shared across the ingestion job to avoid unnecessary overhead
 * of creating multiple sessions.
 */
private final SparkSession spark;

// Reuse a single Validator instance since they are thread-safe and expensive to create
private static final Validator VALIDATOR = Validation.byProvider(...).getValidator();
```

```java
// PASS — obvious flag, no doc needed
private volatile boolean closed = false;
```

Rules:
- Use `/** ... */` Javadoc style above the field for multi-line explanations
- Use `//` inline style for single-line clarifications on constants
- Omit documentation on trivially obvious fields (e.g., simple boolean flags, logger declarations)

---

## 4. Inline Comments — Explain "Why", Never "What"

Write inline comments only when the code does something that would surprise a reader.
The code itself shows **what** is happening; comments explain **why**.

```java
// PASS — explains a non-obvious design decision
// Prevents job crash when Kafka log compaction or retention expiry removes offsets
// the consumer hasn't read yet; such gaps are expected in long-running pipelines.
private static final String OPT_FAIL_ON_DATA_LOSS = "failOnDataLoss";

// PASS — explains platform-specific behavior
// Spark passes kafka.* options directly to the underlying Kafka consumer.
// The suffix is taken from the public ConsumerConfig constants in kafka-clients.
private static final String OPT_BOOTSTRAP_SERVERS = "kafka." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

// PASS — explains a performance constraint
// Reuse a single Validator instance since they are thread-safe and expensive to create
private static final Validator VALIDATOR = ...;
```

```java
// FAIL — restates what the code already says
// Set closed to true
closed = true;

// FAIL — obvious from method name
// Log the info
logger.info("Opening streaming read: topic={}", config.topic());
```

Use section markers to separate internal helpers from the public API:

```java
// -------------------------------------------------------------------------
// Internal helpers
// -------------------------------------------------------------------------
```

---

## 5. Validation Annotations (Config Records and DTOs)

Apply Jakarta Bean Validation annotations on all configuration records and input DTOs.
Follow the pattern established in `IngestionConfig.java` and `KafkaSourceConfig.java`.

```java
// PASS
public record KafkaSourceConfig(
    @NotBlank(message = "KAFKA_BOOTSTRAP_SERVERS must not be blank")
    String bootstrapServers,

    @NotBlank(message = "KAFKA_TOPIC must not be blank")
    String topic,

    @Pattern(regexp = "^(earliest|latest|\\{.*\\})$",
             message = "KAFKA_STARTING_OFFSETS must be 'earliest', 'latest', or a JSON offset map")
    String startingOffsets,

    @Min(value = 1, message = "KAFKA_MAX_OFFSETS_PER_TRIGGER must be at least 1")
    long maxOffsetsPerTrigger
) {}
```

```java
// FAIL — required field with no validation
public record IngestionConfig(String bootstrapServers, ...) {}
```

Rules:
- `@NotBlank` on all required String fields
- `@Pattern(regexp = "...", message = "...")` with a human-readable error message when format matters
- `@Min` / `@Max` on numeric fields with meaningful bounds
- `@Valid` on nested config/record fields to cascade validation
- `@Override` on every method implementing an interface or overriding a superclass method

---

## 6. Special Cases

**Records** — Add class Javadoc. Skip per-component Javadoc; let validation annotations with descriptive `message` fields serve as the component documentation.

**Interfaces** — Javadoc every method. Document contract caveats explicitly:
```java
/**
 * Opens a streaming DataFrame connected to this source.
 * Call this method at most once per source instance.
 *
 * @return a streaming Dataset; never null
 * @throws IllegalStateException if this source has already been closed
 */
Dataset<Row> readStream();
```

**Test classes** — No Javadoc required. Test method names serve as documentation.

**Overridden methods** — Always use `@Override`. Add Javadoc only if the override meaningfully narrows or extends the contract documented on the interface.

---

## Documentation Checklist

Apply this checklist to every new or modified Java file before considering it complete:

- [ ] All public/package-private classes, interfaces, records, and enums have class-level Javadoc with `@author`
- [ ] All public/package-private methods have method-level Javadoc
- [ ] `@param`, `@return`, and `@throws` are present and specific — no placeholder text
- [ ] Non-trivial private fields and constants have field-level documentation
- [ ] Inline comments explain "why", not "what" — no comments that restate the code
- [ ] Validation annotations (`@NotBlank`, `@Pattern`, `@Min`, `@Valid`) applied on config records and DTOs
- [ ] `@Override` present on all interface implementations and superclass overrides
- [ ] Internal helper sections are separated with a section marker comment
