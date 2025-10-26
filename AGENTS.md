# Repository Guidelines

## Project Structure & Module Organization
The Spring Boot MCP server lives under `src/main/java/com/example/kafkamcp`, split into `domain`, `ports`, `application`, and adapter packages (`adapters/kafka`, `adapters/mcp`, `support`). Shared configuration such as `application.yaml` sits in `src/main/resources`. Tests reside in `src/test/java` and mirror the production package layout; integration fixtures for Kafka Testcontainers belong in `src/test/resources`.

## Build, Test, and Development Commands
- `./mvnw clean verify` — compile, run unit tests, and execute integration tests inside Testcontainers.
- `./mvnw spring-boot:run` — launch the MCP server for local development; expects `KAFKA_BOOTSTRAP_SERVERS`.
- `./mvnw test -DskipITs=true` — fast feedback cycle when you only need unit coverage.
- `./mvnw spotless:apply` — format Java sources before committing.

## Coding Style & Naming Conventions
Target Java 21 with four-space indentation and UTF-8 source files. Keep hexagonal boundaries clear: domain records own business rules, adapters wrap Kafka and Spring AI. Use descriptive package names (`com.example.kafkamcp.application.tail`) and prefer immutable types (`record`, `List.copyOf`). Method names should read like use cases (`execute`, `produceMessage`); tests should follow `methodUnderTest_condition_expectedResult`.

## Testing Guidelines
JUnit 5 drives unit tests; integration flows rely on Testcontainers Kafka. Mark slow suites with `@Tag("integration")` so they can be toggled via `-DskipITs=true`. Aim to cover message validation, Kafka error mapping, and MCP payload parsing. When adding new tools, include at least one integration test that exercises the full adapters path and asserts structured responses.

## Commit & Pull Request Guidelines
Use Conventional Commits (`feat:`, `fix:`, `chore:`) with concise subject lines and optional scope (e.g., `feat(produce): enforce payload limits`). Each PR should describe the change, link any tracking issue, and mention new env vars or limits. Include screenshots or terminal snippets when behavior changes the MCP surface. Before requesting review, run `./mvnw clean verify` and ensure formatting passes.

## Security & Configuration Tips
Never hard-code bootstrap servers or credentials; rely on `application.yaml` placeholders and environment overrides. Keep Testcontainers versions aligned with Maven BOM updates to receive security patches. For new Kafka capabilities, validate caps (`messagesPerCall`, `bytesPerCall`) against plan.md before exposing them via MCP tools.
