---
name: feature-dev
description: Structured methodology for implementing new features: define context and acceptance criteria, stub classes/functions, write failing tests (TDD), then implement incrementally.
---

# Feature Development Methodology

A disciplined, four-phase approach to implementing new features that prevents scope creep, catches design problems early, and produces code that is correct by construction.

## When to Activate

- Starting implementation of any new class, interface, or non-trivial function
- Asked to "implement X", "add support for Y", or "wire up Z"
- Beginning work described by a ticket, issue, or spec section
- Scaffolding a new service, pipeline stage, handler, or source adapter
- Picking up a stub left by a previous phase

## Phase 1: Context and Acceptance Criteria

**Write this down before touching any production file.**

### 1.1 Problem Statement

State in plain language:
- What capability is missing or broken?
- Who or what is affected?
- What does the system look like after this change?

```
# EXAMPLE — problem statement for KafkaSource.readStream()
#
# Gap: KafkaSource.readStream() is stubbed and throws UnsupportedOperationException.
# Affected: IngestionJob cannot start a streaming pipeline.
# After fix: readStream() returns a Spark streaming DataFrame connected to the
#            configured Kafka topic, using the bootstrap servers and group ID
#            from KafkaSourceConfig.
```

### 1.2 Acceptance Criteria

List specific, testable conditions for "done". Each criterion should map to at least one test.

```
# EXAMPLE — acceptance criteria
#
# AC-1: readStream() returns a DataFrame where isStreaming() == true
# AC-2: The DataFrame schema matches SchemaRegistry.forTopic(config.topic())
# AC-3: Kafka options (bootstrapServers, subscribe, groupId, startingOffsets)
#        are forwarded to the Spark reader exactly as specified in config
# AC-4: Calling readStream() after close() throws IllegalStateException
# AC-5: If bootstrapServers is blank the constructor rejects the config via
#        Bean Validation before readStream() is ever called
```

### 1.3 Constraints and Known Error Conditions

Surface every failure mode before writing a line of code.

```
# EXAMPLE — constraints and error conditions
#
# CONSTRAINT: KafkaSource must not hold a SparkSession reference at construction
#             time; the session is injected via readStream(SparkSession) to keep
#             the record serializable.
#
# ERROR: Kafka broker unreachable           → surface as LakehouseException
# ERROR: Topic does not exist              → Spark throws AnalysisException; wrap it
# ERROR: Invalid startingOffsets JSON      → reject at config validation time
# ERROR: readStream() called after close() → throw IllegalStateException
```

---

## Phase 2: Stubs

Create the skeleton — correct signatures, return types, and Javadoc — with no logic. Every known error condition gets a `@throws` tag.

```java
// STUB: bodies intentionally unimplemented — Phase 3 tests will drive the real code
public class WidgetProcessor implements Processor<Widget> {

  private final WidgetRepository repository;
  private volatile boolean closed = false;

  public WidgetProcessor(WidgetRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
  }

  /**
   * Processes a widget and persists the result.
   *
   * @param widget the widget to process; must not be null
   * @return the persisted result record
   * @throws IllegalArgumentException if widget is null or its id is blank
   * @throws IllegalStateException    if this processor has been closed
   * @throws ProcessingException      if the repository write fails
   */
  @Override
  public Result process(Widget widget) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  /**
   * Releases any held resources. Idempotent.
   */
  @Override
  public void close() {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
```

**Stub rules:**
- Throw `UnsupportedOperationException("not yet implemented")` — never `null` returns or silent no-ops
- Put `@throws` on every error condition identified in Phase 1.3
- Identify all collaborators now (constructor parameters, injected dependencies) so tests can prepare them
- Do not implement any logic — a stub that accidentally passes a test is a false Green

---

## Phase 3: Failing Tests (Red)

Write tests that describe the **intended behavior** before implementing it. Run the suite — every new test must fail. A test that passes against a stub signals either a logic mistake in the stub or a test that does not actually exercise the contract.

### Test structure

```java
class WidgetProcessorTest {

  // Collaborators
  private WidgetRepository repository;
  private WidgetProcessor processor;

  @BeforeEach
  void setUp() {
    repository = mock(WidgetRepository.class);  // or a real in-memory impl
    processor  = new WidgetProcessor(repository);
  }

  // AC-1: happy path
  @Test
  void process_withValidWidget_returnsPersistedResult() {
    var widget = new Widget("w-1", "sprocket");
    var expected = new Result("r-1", "w-1");
    when(repository.save(any())).thenReturn(expected);

    var result = processor.process(widget);  // RED: throws UnsupportedOperationException

    assertThat(result).isEqualTo(expected);
    verify(repository).save(any());
  }

  // AC-2: null guard
  @Test
  void process_withNullWidget_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> processor.process(null))  // RED
        .isInstanceOf(IllegalArgumentException.class);
  }

  // AC-3: closed state
  @Test
  void process_afterClose_throwsIllegalStateException() {
    processor.close();

    assertThatThrownBy(() -> processor.process(new Widget("w-1", "sprocket")))  // RED
        .isInstanceOf(IllegalStateException.class);
  }

  // AC-4: repository failure surfaced as domain exception
  @Test
  void process_whenRepositoryThrows_throwsProcessingException() {
    when(repository.save(any())).thenThrow(new RuntimeException("db down"));

    assertThatThrownBy(() -> processor.process(new Widget("w-2", "gear")))  // RED
        .isInstanceOf(ProcessingException.class)
        .hasMessageContaining("db down");
  }
}
```

**Red phase rules:**
- One test per acceptance criterion minimum; add edge-case tests freely
- Test names follow `<method>_<condition>_<expected outcome>` (AssertJ / JUnit 5 style)
- Run with `./gradlew test` — confirm every new test fails before moving on
- Do not adjust the stub to make tests pass; fix tests only if the criterion itself was wrong

---

## Phase 4: Implement and Refactor (Green → Refactor)

Implement one class or function at a time, in dependency order. Run the full suite after each unit.

### Green: make tests pass

```java
// IMPLEMENTATION — replaces stubs one method at a time
@Override
public Result process(Widget widget) {
  if (closed) throw new IllegalStateException("processor is closed");
  if (widget == null || widget.id().isBlank()) {
    throw new IllegalArgumentException("widget must be non-null with a non-blank id");
  }
  try {
    return repository.save(toRecord(widget));
  } catch (Exception ex) {
    throw new ProcessingException("failed to persist widget " + widget.id(), ex);
  }
}

@Override
public void close() {
  closed = true;
}
```

After adding each method body, run `./gradlew test`. The corresponding tests should flip from Red to Green. If a previously passing test starts failing, stop and investigate before continuing.

### Refactor: improve without breaking

Once all tests are Green:
- Remove duplication across methods
- Rename for clarity
- Extract private helpers if a method is doing more than one thing
- Improve logging and error messages

Run the full suite after every refactor step. Tests must stay Green throughout.

### When to refactor tests

Refactor a test only when the **interface contract** changed — a renamed method, a different parameter type, a changed exception type. Never refactor a test to make it pass without also updating the implementation.

```java
// PASS: refactor test because the method signature changed
// was: processor.process(Widget widget)
// now: processor.process(Widget widget, ProcessingContext ctx)
@Test
void process_withValidWidget_returnsPersistedResult() {
  var ctx = ProcessingContext.defaultContext();  // added to match new signature
  var result = processor.process(widget, ctx);
  assertThat(result).isEqualTo(expected);
}

// FAIL: do not change the assertion just to silence a failing test
assertThat(result).isNotNull(); // ← weakened assertion hides a real bug
```

---

## Feature Development Checklist

- [ ] Problem statement written before any production file is opened
- [ ] Acceptance criteria listed; each maps to at least one named test
- [ ] All error conditions documented in Phase 1.3 and reflected in `@throws` Javadoc
- [ ] Stubs created with `UnsupportedOperationException("not yet implemented")` bodies
- [ ] Tests written and run — every new test fails (Red) before implementation starts
- [ ] Each class/function implemented one at a time; suite run after each
- [ ] All tests pass (Green) before any refactoring begins
- [ ] Tests refactored only when the interface contract changed, not to paper over failures
- [ ] No `UnsupportedOperationException` stubs remain in production code
- [ ] `./gradlew build` passes clean before marking the feature complete
