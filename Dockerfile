# syntax=docker/dockerfile:1.7

###### Builder stage ###########################################################
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace

# Pre-copy Maven wrapper and pom to leverage Docker layer caching
COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN chmod +x mvnw && ./mvnw -B -ntp dependency:go-offline

# Copy source and build the application
COPY src src
RUN ./mvnw -B -ntp clean package -DskipTests

###### Runtime stage ###########################################################
FROM eclipse-temurin:21-jre AS runtime
ENV APP_HOME=/app \
    JAVA_OPTS="" \
    SPRING_PROFILES_ACTIVE=default
WORKDIR ${APP_HOME}

# Create non-root user for the app
RUN useradd --system --create-home --home-dir ${APP_HOME} appuser

# Copy shaded jar from builder
ARG JAR_FILE=target/kafka-mcp-0.1.0-SNAPSHOT.jar
COPY --from=build /workspace/${JAR_FILE} app.jar
RUN chown appuser:appuser app.jar

USER appuser
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
