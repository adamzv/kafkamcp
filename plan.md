# Kafka MCP Server — Coder Requirements & Implementation Plan (Spring Boot + Spring AI MCP)

## 0) Decisions (locked for v1)

* **Platform:** Java 21, **Spring Boot 3.3+**, **Spring AI MCP Server** (STDIO transport).
* **Kafka security:** **Unsecured** (no SASL/TLS) for v1. Future v1.1 adds SASL/ACL without breaking APIs.
* **Payload formats:** `raw-string` and `json` (validate on produce, parse on tail).
* **Kafka ops in v1:** Read-only + produce. No admin mutations (create/delete topic, reset offsets).
* **MCP transport:** **STDIO-only**.
* **Caps:** `messagesPerCall = 200`, `bytesPerCall = 1 MiB`, `messageBytes = 256 KiB`.
* **Config:** Via `application.yaml`; secrets/servers from **env vars**.

---

## 1) Deliverables

* Spring Boot app exposing **MCP tools** via Spring AI MCP (STDIO).
* Clean hexagonal code: **domain + ports + adapters + application (use-cases)**.
* Integration tests with **Testcontainers/Kafka**.
* Minimal **README** with run instructions.

---

## 2) Dependencies (Maven examples)

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter</artifactId>
</dependency>
<dependency>
  <groupId>org.springframework.ai</groupId>
  <artifactId>spring-ai-starter-mcp-server</artifactId>
  <version>1.0.0</version>
</dependency>
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>3.7.0</version>
</dependency>
<!-- Observability (optional Prometheus) -->
<dependency>
  <groupId>io.micrometer</groupId>
  <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
<!-- Tests -->
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>kafka</artifactId>
  <version>1.20.1</version>
  <scope>test</scope>
</dependency>
```

---

## 3) Configuration (Spring Boot + Spring AI MCP)

**application.yaml**

```yaml
spring:
  application:
    name: kafka-mcp
  ai:
    mcp:
      server:
        name: kafka-mcp
        version: 0.1.0
        stdio: true
        capabilities:
          tool: true
          resource: false
          prompt: false
        instructions: >
          Read and produce to Kafka with bounded results.
          Formats: raw-string, json. No admin mutations.

kafka:
  bootstrapServers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

limits:
  messagesPerCall: ${LIMIT_MESSAGES_PER_CALL:200}
  bytesPerCall: ${LIMIT_BYTES_PER_CALL:1048576}   # 1 MiB
  messageBytes: ${LIMIT_MESSAGE_BYTES:262144}     # 256 KiB

# placeholders for v1.1 (SASL/TLS) – do not use yet
# kafka.security.sasl.enabled: ${KAFKA_SASL_ENABLED:false}
# ...
```

**Startup validation**

* Fail fast if `kafka.bootstrapServers` is blank.
* Log MCP server name/version and limits on startup.

---

## 4) Architecture (Hexagonal)

```
com.example.kafkamcp
├─ domain/                 // pure domain records + error model
├─ ports/                  // interfaces (KafkaAdminPort, KafkaProducerPort, KafkaConsumerPort)
├─ application/            // use-cases (orchestrate ports, validate, map errors)
├─ adapters/
│  ├─ kafka/               // Admin/Producer/Consumer adapters (kafka-clients)
│  └─ mcp/                 // MCP tool endpoints (Spring AI @Tool methods)
└─ support/                // Config (@ConfigurationProperties), ObjectMapper, logging, limits
```

**Clean code requirements**

* Constructor injection only. No field/static injection.
* Small classes/methods, single responsibility, pure helpers, immutability by default (`record`, `List.copyOf`, `Map.copyOf`).
* Strict validation at boundaries (MCP endpoints and use-cases).
* Do not leak Kafka exceptions/objects outside adapters.

---

## 5) Domain Model (immutable records)

```java
public record ProduceRequest(String topic, String format, String key,
                             Map<String,String> headers, String value) {}
public record ProduceResult(String topic, int partition, long offset, long timestamp) {}

public record TailRequest(String topic, String from, Integer limit, Integer partition) {}
public record MessageEnvelope(String key, Map<String,String> headers,
                              String valueString, Object valueJson,
                              long timestamp, int partition, long offset) {}

public record TopicInfo(String name, int partitions, short replication, boolean internal) {}

public record Limits(int messagesPerCall, int bytesPerCall, int messageBytes) {}
```

**Error model**

```java
public record Problem(String code, String message, Map<String,Object> details) {}
public class ProblemException extends RuntimeException {
  private final Problem problem;
  public ProblemException(Problem p) { super(p.message()); this.problem = p; }
  public Problem problem() { return problem; }
}
```

**Codes:** `INVALID_ARGUMENT`, `PAYLOAD_TOO_LARGE`, `NOT_FOUND`, `KAFKA_UNAVAILABLE`, `OPERATION_FAILED`.

---

## 6) Ports (interfaces to implement)

```java
public interface KafkaAdminPort {
  Set<String> listTopicNames();
  Map<String,TopicInfo> describeTopics(Collection<String> topicNames);
  List<Map<String,Object>> listConsumerGroups(String prefix); // prefix may be null
}
public interface KafkaProducerPort {
  ProduceResult produce(ProduceRequest req) throws ProblemException;
}
public interface KafkaConsumerPort {
  List<MessageEnvelope> tail(TailRequest req, Limits limits) throws ProblemException;
}
```

---

## 7) Use-Cases (application layer)

### 7.1 ListTopicsUseCase

* Input: optional `prefix`.
* Steps: `listTopicNames()` → filter prefix → `describeTopics(filtered)` → sort by name.
* Errors: map adapter failures to `KAFKA_UNAVAILABLE`.

### 7.2 DescribeTopicUseCase

* Input: `topic`.
* Steps: `describeTopics(List.of(topic))` → return `{topic, partitions: [...]}`.
* Errors: missing topic → `NOT_FOUND`.

### 7.3 ProduceMessageUseCase

* Validate: topic nonblank; value nonnull; size ≤ `messageBytes`; if `format=json` → parse with `ObjectMapper`.
* Steps: call `KafkaProducerPort.produce(req)`.
* Errors: invalid JSON → `INVALID_ARGUMENT`; too large → `PAYLOAD_TOO_LARGE`.

### 7.4 TailTopicUseCase

* `from` grammar: `end-N` (default N=50 if null), `offset:X`, `timestamp:epochMillis`.
* Validate: grammar + non-negative numbers; clamp `limit` to `messagesPerCall`.
* Steps: `KafkaConsumerPort.tail(req, limits)`; parse JSON for `valueJson` if applicable.
* Errors: bad grammar → `INVALID_ARGUMENT`; unknown topic/partition → `NOT_FOUND`.

### 7.5 ListConsumerGroupsUseCase

* Input: optional `prefix`.
* Output: `[{groupId,state,members}]`.

---

## 8) Kafka Adapters (implementation rules)

### 8.1 Admin Adapter

* Build one `AdminClient`.
* `listTopicNames()` → `admin.listTopics().names()` with timeout.
* `describeTopics()` → bulk describe; `replication` from first partition’s replicas; `internal = name.startsWith("__")`.
* Map exceptions: connectivity → `KAFKA_UNAVAILABLE`; missing topic → `NOT_FOUND`.

### 8.2 Producer Adapter

* Single shared `Producer<String,String>`.
* Build `ProducerRecord` with optional UTF-8 headers (null header values → empty byte[]).
* `send(...).get(timeout)` to fail deterministically; map `RecordTooLargeException` to `PAYLOAD_TOO_LARGE`.

### 8.3 Consumer Adapter (tail)

* Create **short-lived** `Consumer<String,String>` per call; always close with try-with-resources.
* Assign partitions:

  * If `partition` provided: `assign([tp])`
  * Else: fetch partitions via Admin; `assign(all)`
* Seek:

  * `end-N`: `endOffsets` then `seek(end-N)` per TP (N default 50)
  * `offset:X`: `seek(tp, X)`
  * `timestamp:T`: `offsetsForTimes` then `seek`
* Poll loop (bounded):

  * Poll 300ms slices until `limit` or `bytesPerCall` budget is exhausted.
  * Build `MessageEnvelope` (parse JSON with ObjectMapper; if parse ok, fill `valueJson`, else null).
  * **Do not commit offsets.**

**Time bounds:** add per-call time budget (e.g., 5–10s) to avoid endless loops.

---

## 9) MCP Tool Endpoints (Spring AI)

Bind use-cases behind **Spring AI MCP** `@Tool` methods. Convert `ProblemException` → MCP tool error `{code,message,details}`.

**Tool surface (exact):**

* `listTopics(prefix?) -> TopicInfo[]`
* `describeTopic(topic) -> { topic, partitions: [{partition,leader,isr[],replicas[]}] }`
* `produceMessage({topic,format,key?,headers?,value}) -> {topic,partition,offset,timestamp}`
* `tailTopic({topic,from,limit?,partition?}) -> MessageEnvelope[]`
* `listConsumerGroups(prefix?) -> [{groupId,state,members}]`

**Examples (MCP usage)**

```json
// produce string
{"topic":"demo","format":"raw-string","value":"hello world"}

// produce json
{"topic":"demo","format":"json","value":"{\"id\":1,\"name\":\"Ada\"}"}

// tail last 50
{"topic":"demo","from":"end-50"}

// tail from timestamp
{"topic":"demo","from":"timestamp:1733980000000","limit":100}
```

---

## 10) Logging, Metrics, Tracing

* **Structured logs** per tool call: requestId, tool, topic(s), limit, bytesOut, count, duration, outcome.
* **Micrometer**:

  * `kafka_mcp_tool_duration_seconds{tool=...}` (Timer)
  * `kafka_mcp_tool_errors_total{tool,code}` (Counter)
  * `kafka_mcp_bytes_out_total{tool}` and `kafka_mcp_messages_out_total{tool}` (Counters)
* MDC during tail loop: `topic`, `partition`, `offset` for any per-record warnings.

---

## 11) Acceptance Criteria

* **listTopics:** returns `TopicInfo[]`; `prefix` filters correctly.
* **describeTopic:** includes partitions with leader/ISR/replicas; unknown → `NOT_FOUND`.
* **produceMessage:** rejects invalid JSON / oversize; success returns non-negative offset + timestamp.
* **tailTopic:** honors `from` grammar; clamps to `limit` and budgets; parses JSON when valid.
* **listConsumerGroups:** returns groups without error on empty clusters.
* Startup fails if `bootstrapServers` missing or blank.

---

## 12) Test Plan

* **Unit tests (use-cases):** validation, error mapping, `from` grammar (`end-N|offset:X|timestamp:T`).
* **Integration (Testcontainers):**

  * Produce 3 strings + 2 jsons → tail `end-5` returns 5; 2 with `valueJson != null`.
  * Oversize produce → `PAYLOAD_TOO_LARGE`.
  * DescribeTopic reflects replica count & partition metadata.
  * ListConsumerGroups shows a spawned test consumer group.
* **Property/boundary:** randomized `N`, `limit` within caps; timeouts honored.

---

## 13) Coding Standards & Best Practices

* **SOLID / Hexagonal**; no business logic in adapters.
* **Constructor injection**, **final fields**, no statics/singletons.
* **Immutability** for outputs; no exposure of mutable Kafka types.
* **Small methods**, single abstraction level; extract helpers for seek/validation/mapping.
* **No `System.*` in core logic** (inject `Clock` if time is needed).
* **Clear error messages** for `Problem.message`; diagnostics go to logs, not to tool responses.
* **Threading:** rely on Kafka client’s internal threads; avoid custom executors for v1.
* **Time bounds:** add configurable timeouts for Admin/Producer/Consumer operations.

---

## 14) Runbook (dev)

* Env: `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
* Run Spring Boot app; MCP client connects **via STDIO** (e.g., Claude Desktop “local MCP server”).
* Try: `produceMessage` → `tailTopic` → `listTopics`.
