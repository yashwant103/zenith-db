# ── STAGE 1: Build fat JAR ────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-21 AS builder
WORKDIR /build

# Copy pom.xml first — Docker caches this layer separately
# If only source changes, dependencies are NOT re-downloaded
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Copy source and build fat JAR with all dependencies bundled
COPY src ./src
RUN mvn clean package -DskipTests --enable-preview -q

# ── STAGE 2: Minimal runtime image ────────────────────────
# alpine = 50MB vs jammy = 200MB — smaller = faster pull on EC2
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Non-root user — required by bank security compliance
RUN addgroup -S zenith && adduser -S zenith -G zenith

# Copy ONLY the fat JAR — no source, no Maven, no JDK
COPY --from=builder /build/target/zenith-db-1.0-SNAPSHOT.jar app.jar

# WAL log directory — mounted as volume so data persists
RUN mkdir -p /data && chown zenith:zenith /data
USER zenith

# Raft + client port
EXPOSE 9001 9002 9003

# Metrics port (Prometheus scrapes this)
EXPOSE 8080

# WAL log persists here across container restarts
VOLUME ["/data"]

# --enable-preview required for pattern matching in switch (Java 21)
# Working dir = /data so zenith_db.log is written to the volume
WORKDIR /data
ENTRYPOINT ["java", "--enable-preview", "-Xms64m", "-Xmx256m", "-jar", "/app/app.jar"]