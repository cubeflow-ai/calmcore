# CalmDB Docker Image
# Multi-stage build for smaller image size

# Stage 1: Build
FROM rust:1.75-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy Cargo files first for better caching
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY libs ./libs
COPY examples ./examples
COPY tests ./tests

# Build release binary
RUN cargo build --release --bin calm

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 calm

# Create directories
RUN mkdir -p /data /logs /config && chown -R calm:calm /data /logs /config

# Copy binary from builder
COPY --from=builder /app/target/release/calm /usr/local/bin/calm

# Set permissions
RUN chmod +x /usr/local/bin/calm

# Switch to non-root user
USER calm

# Default environment variables
ENV CALM_HOST=0.0.0.0 \
    CALM_DATA_DIR=/data \
    CALM_LOG_FILE=/logs/calm.log \
    CALM_LOG_LEVEL=info \
    CALM_GRAPHQL_PORT=9567 \
    CALM_ES_PORT=9200 \
    CALM_MYSQL_PORT=3307

# Expose ports
# GraphQL: 9567, Elasticsearch: 9200, MySQL: 3307, Gossip: 7946, RPC: 7947
EXPOSE 9567 9200 3307 7946/udp 7946/tcp 7947

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9567/ || exit 1

# Default command
CMD ["calm"]
