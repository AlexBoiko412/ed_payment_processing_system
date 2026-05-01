# Multi-stage Dockerfile - shared by all six Spring Boot services.
# Build a specific service with: docker build --build-arg SERVICE_NAME=payment-service .
# docker-compose sets SERVICE_NAME automatically per service entry.

# Stage 1: Dependency cache
FROM maven:3.9.6-eclipse-temurin-21 AS deps
WORKDIR /build

COPY pom.xml .
COPY shared-lib/pom.xml           shared-lib/
COPY api-gateway/pom.xml          api-gateway/
COPY payment-service/pom.xml      payment-service/
COPY fraud-service/pom.xml        fraud-service/
COPY ledger-service/pom.xml       ledger-service/
COPY notification-service/pom.xml notification-service/
COPY settlement-service/pom.xml   settlement-service/

RUN mvn dependency:go-offline --no-transfer-progress -q

# Stage 2: Build
FROM deps AS builder
ARG SERVICE_NAME
ARG SSL_KEYSTORE_PASSWORD=changeit

COPY shared-lib/src           shared-lib/src
COPY api-gateway/src          api-gateway/src
COPY payment-service/src      payment-service/src
COPY fraud-service/src        fraud-service/src
COPY ledger-service/src       ledger-service/src
COPY notification-service/src notification-service/src
COPY settlement-service/src   settlement-service/src

# Generate a self-signed PKCS12 keystore for HTTPS.
# keytool is bundled with the JDK in this base image
RUN keytool -genkeypair \
    -alias ${SERVICE_NAME} \
    -keyalg RSA -keysize 2048 \
    -storetype PKCS12 \
    -keystore ${SERVICE_NAME}/src/main/resources/keystore.p12 \
    -validity 3650 \
    -storepass "${SSL_KEYSTORE_PASSWORD}" \
    -dname "CN=localhost, OU=Dev, O=Payment, C=US" \
    -noprompt 2>/dev/null && \
    echo "Keystore generated for ${SERVICE_NAME}"

# Build
RUN mvn clean package \
    -pl shared-lib,${SERVICE_NAME} -am \
    -DskipTests \
    --no-transfer-progress -q

# Stage 3: Runtime
FROM eclipse-temurin:21-jre-alpine AS runtime
ARG SERVICE_NAME
WORKDIR /app

# curl is used by the docker-compose health check
RUN apk add --no-cache curl && \
    addgroup -S payment && \
    adduser -S payment -G payment

COPY --from=builder /build/${SERVICE_NAME}/target/*.jar app.jar
RUN chown payment:payment app.jar

USER payment

ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-jar", "app.jar"]
