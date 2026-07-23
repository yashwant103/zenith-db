# ── STAGE 1: Build fat JAR ────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-21 AS builder
WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn clean package -DskipTests -q

# Show what was built — useful for debugging
RUN echo "=== JARs in target/ ===" && ls -lh target/*.jar

# ── STAGE 2: Minimal runtime image ────────────────────────
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S zenith && adduser -S zenith -G zenith

# netcat required for healthcheck: nc -z localhost PORT
RUN apk add --no-cache netcat-openbsd

# FIX: Copy the fat JAR directly as app.jar in one step.
# Previous approach: COPY *.jar ./ → mv → rm -f *.jar
# BUG in that approach: rm -f *.jar deleted app.jar itself after the rename!
# Solution: copy and rename in a single COPY instruction — no shell script needed.
# maven-shade-plugin names the fat JAR: zenith-db-1.0-SNAPSHOT.jar
# (the thin original gets prefixed: original-zenith-db-1.0-SNAPSHOT.jar)
COPY --from=builder /build/target/zenith-db-1.0-SNAPSHOT.jar app.jar

# Verify the JAR exists and is readable before proceeding
RUN ls -lh app.jar && echo "JAR copy successful"

RUN mkdir -p /data && chown zenith:zenith /data
USER zenith

EXPOSE 9001 9002 9003
EXPOSE 8080

VOLUME ["/data"]

WORKDIR /data
ENTRYPOINT ["java", "--enable-preview", "-Xms512m", "-Xmx1024m", "-jar", "/app/app.jar"]