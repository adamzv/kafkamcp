# Kafka MCP Server

Spring Boot 3.3 / Java 21 MCP server that exposes Kafka read/produce tooling over the Spring AI MCP STDIO transport and an HTTP SSE endpoint. The app follows a hexagonal layout (`domain`, `ports`, `application`, `adapters`) and relies on Kafka clients plus Testcontainers for end-to-end verification.

## Architecture

The project follows hexagonal architecture (ports and adapters) to keep business logic independent from external concerns.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MCP CLIENT LAYER                                   │
│                     ┌─────────────────────────┐                             │
│                     │   MCP Client            │                             │
│                     │   (Claude Code)         │                             │
│                     └───────────┬─────────────┘                             │
└─────────────────────────────────┼───────────────────────────────────────────┘
                                  │ HTTP SSE
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       ADAPTERS - INBOUND                                    │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  SSE Transport (WebMvcSseServerTransportProvider)                    │  │
│  └────────────────────────────────┬─────────────────────────────────────┘  │
│                                   │                                         │
│  ┌────────────────────────────────▼─────────────────────────────────────┐  │
│  │  KafkaTools (@Tool Methods)                                          │  │
│  │  - listTopics()      - tailTopic()                                   │  │
│  │  - describeTopic()   - produceMessage()                              │  │
│  │  - listConsumerGroups()                                              │  │
│  └──┬────────────┬────────────┬────────────┬────────────┬───────────────┘  │
└─────┼────────────┼────────────┼────────────┼────────────┼──────────────────┘
      │            │            │            │            │
      │            │            │            │            │
┌─────▼────────────▼────────────▼────────────▼────────────▼──────────────────┐
│                     APPLICATION LAYER - USE CASES                           │
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ ListTopics   │  │ DescribeTopic│  │ ListConsumer │  │ TailTopic     │  │
│  │ UseCase      │  │ UseCase      │  │ GroupsUseCase│  │ UseCase       │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬────────┘  │
│         │                 │                  │                  │           │
│         │                 │                  │                  │           │
│  ┌──────────────┐         │                  │                  │           │
│  │ ProduceMsg   │         │                  │                  │           │
│  │ UseCase      │         │                  │                  │           │
│  └──────┬───────┘         │                  │                  │           │
└─────────┼─────────────────┼──────────────────┼──────────────────┼───────────┘
          │                 │                  │                  │
          │      ┌──────────┴──────────────────┘                  │
          │      │                                                │
          │      │                                  ┌─────────────┘
          │      │                                  │
┌─────────▼──────▼──────────────────────────────────▼─────────────────────────┐
│                        PORTS - INTERFACES                                   │
│                                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐         │
│  │ KafkaProducerPort│  │ KafkaAdminPort   │  │ KafkaConsumerPort│         │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘         │
└───────────┼────────────────────────┼──────────────────────┼─────────────────┘
            │                        │                      │
            │ implements             │ implements           │ implements
            │                        │                      │
┌───────────▼────────────────────────▼──────────────────────▼─────────────────┐
│                      ADAPTERS - OUTBOUND                                    │
│                                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐         │
│  │ KafkaProducer    │  │ KafkaAdmin       │  │ KafkaConsumer    │         │
│  │ Adapter          │  │ Adapter          │  │ Adapter          │         │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘         │
└───────────┼────────────────────────┼──────────────────────┼─────────────────┘
            │                        │                      │
            │ KafkaProducer          │ AdminClient          │ KafkaConsumer
            │                        │                      │
            └────────────────────────┼──────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL SYSTEMS                                    │
│                     ┌─────────────────────────┐                             │
│                     │   Apache Kafka Cluster  │                             │
│                     └─────────────────────────┘                             │
└─────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────┐
│                           DOMAIN LAYER                                      │
│                                                                             │
│  Domain Models (Records):                                                  │
│  • TopicInfo          • MessageEnvelope    • ProduceRequest                │
│  • TailRequest        • ProduceResult      • TopicDescriptionResult        │
│  • Limits             • Problem            • ProblemException              │
│                                                                             │
│  Used by all Use Cases for data contracts and business rules               │
└─────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONFIGURATION & SUPPORT                                  │
│                                                                             │
│  • ApplicationConfig      - Wires Kafka clients and MCP tools              │
│  • SseTransportConfig     - Configures SSE transport provider              │
│  • KafkaProperties        - Kafka bootstrap servers configuration          │
│  • LimitsProperties       - Message/byte limits configuration              │
│  • StartupLogger          - Logs server readiness                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

- **Domain Layer**: Pure business logic, immutable data models (records), no external dependencies
- **Ports**: Interfaces defining contracts between application and adapters
- **Application Layer**: Use cases orchestrating business operations, enforcing limits and validation
- **Adapters (Inbound)**: MCP tool exposure via Spring AI annotations
- **Adapters (Outbound)**: Kafka client implementations (AdminClient, Consumer, Producer)
- **Configuration**: Spring Boot wiring, properties management, bean definitions

## Getting Started

### Prerequisites
- JDK 21
- Docker (only needed when running the Testcontainers-based integration suite)
- A Kafka cluster (or local broker) reachable via `KAFKA_BOOTSTRAP_SERVERS`

### Configuration
All settings live in `src/main/resources/application.yaml`. Override the important ones via environment variables:

| Property                 | Env Var                   | Default          |
|--------------------------|---------------------------|------------------|
| `kafka.bootstrapServers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `limits.messagesPerCall` | `LIMIT_MESSAGES_PER_CALL` | `200`            |
| `limits.bytesPerCall`    | `LIMIT_BYTES_PER_CALL`    | `1048576`        |
| `limits.messageBytes`    | `LIMIT_MESSAGE_BYTES`     | `262144`         |
| `server.port`            | `SERVER_PORT`             | `8080`           |
| `spring.ai.mcp.server.stdio` | n/a | `false` (enables SSE) |
| `spring.ai.mcp.server.base-url` | `MCP_BASE_URL` | `http://localhost:8080` |
| `management.endpoints.web.exposure.include` | n/a | `prometheus,health,info` |

### Run the MCP Server
```bash
./mvnw spring-boot:run \
  -Dspring-boot.run.jvmArguments="-Dspring.main.lazy-initialization=false"
```
- Spring AI currently supports one MCP transport per process; this project ships with the **HTTP SSE transport** enabled (`spring.ai.mcp.server.stdio=false`). To switch back to STDIO, set `spring.ai.mcp.server.stdio=true` and disable/remove the SSE-specific properties.
- HTTP MCP clients should open an SSE stream to `http://localhost:${SERVER_PORT:-8080}/sse` and POST MCP requests to `http://localhost:${SERVER_PORT:-8080}/mcp/message` (both endpoints are configurable via `spring.ai.mcp.server.sse-*` properties).

On startup you’ll see a `mcp_server_ready` log line with the effective limits, bootstrap servers, and MCP metadata.

### Metrics

The Prometheus registry is on the classpath and exposed through Spring Boot Actuator. Once the app is running you can scrape:

```
http://localhost:${SERVER_PORT:-8080}/actuator/prometheus
```

Metrics are emitted per MCP tool (`kafka_mcp_*`) and include durations, counts, bytes, and error tallies.

## Available MCP Tools

| Tool                                                 | Description                                                                                              |
|------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `listTopics(prefix?)`                                | Lists topics (internal ones included) and basic stats.                                                   |
| `describeTopic({topic})`                             | Returns partition metadata with leader, replicas, and ISR IDs.                                           |
| `produceMessage({topic,format,key?,headers?,value})` | Validates payload size/format and produces via Kafka.                                                    |
| `tailTopic({topic,from,limit?,partition?})`          | Tails messages from `earliest`, `latest`, `end-N`, `offset:X`, or `timestamp:T` positions and adds JSON parsing when possible. |
| `listConsumerGroups(prefix?)`                        | Lists consumer groups, states, and members.                                                              |

Each invocation is logged with `tool_call` structured logs and measured via Micrometer timers/counters (`kafka_mcp_*` metrics). Attach the Prometheus registry (already on the classpath) to scrape these metrics in production.

## Testing

- **Unit tests only:** `./mvnw test -DskipITs=true`
- **Full verification (includes Testcontainers Kafka):** `./mvnw clean verify`
- **Formatting:** `./mvnw spotless:apply` (if you add Spotless later; for now follow the repo style guide)

The integration suite spins up Kafka in Docker and covers produce/tail/describe happy paths. Make sure Docker is running before executing the full verify command.

## Development Notes
- Keep the hexagonal boundaries intact: business rules live in `application` and `domain`, while adapters wrap Kafka and MCP transport concerns.
- Constructor injection only; prefer immutable records (`record`) for data contracts.
- Do not hard-code credentials or bootstrap servers—always reference configuration properties.
- When introducing new tools or Kafka capabilities, amend `plan.md` and expand the Testcontainers coverage accordingly.
