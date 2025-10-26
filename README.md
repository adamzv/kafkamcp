# Kafka MCP Server

Spring Boot 3.3 / Java 21 MCP server that exposes Kafka read/produce tooling over the Spring AI MCP STDIO transport. The app follows a hexagonal layout (`domain`, `ports`, `application`, `adapters`) and relies on Kafka clients plus Testcontainers for end-to-end verification.

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

### Run the MCP Server
```bash
./mvnw spring-boot:run \
  -Dspring-boot.run.jvmArguments="-Dspring.main.lazy-initialization=false"
```
The process listens on STDIO for MCP clients (e.g. Claude Desktop). On startup you’ll see a `mcp_server_ready` log line with the effective limits and bootstrap servers.

## Available MCP Tools

| Tool                                                 | Description                                                                                              |
|------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `listTopics(prefix?)`                                | Lists topics (internal ones included) and basic stats.                                                   |
| `describeTopic({topic})`                             | Returns partition metadata with leader, replicas, and ISR IDs.                                           |
| `produceMessage({topic,format,key?,headers?,value})` | Validates payload size/format and produces via Kafka.                                                    |
| `tailTopic({topic,from,limit?,partition?})`          | Tails messages from `end-N`, `offset:X`, or `timestamp:T` positions and adds JSON parsing when possible. |
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
