################################################################################
# Build Stage - Uses Rust stable slim image for faster builds
################################################################################
FROM rust:1.90-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests for dependency caching
COPY Cargo.toml Cargo.lock ./

# Build dependencies only (cache layer)
# Create dummy main.rs to satisfy cargo build
RUN mkdir src && \
    echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy source code
COPY src ./src
COPY build.rs ./build.rs
COPY migrations ./migrations
COPY .sqlx ./.sqlx

# Build the actual application with SQLx offline mode
# Touch main.rs to force recompilation of the binary
ENV SQLX_OFFLINE=true
RUN touch src/main.rs && \
    cargo build --release && \
    strip target/release/blinky

################################################################################
# Runtime Stage - Minimal Debian slim image for security
################################################################################
FROM debian:bookworm-slim

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r blinky && useradd -r -g blinky blinky

# Copy the binary from builder
COPY --from=builder /app/target/release/blinky /usr/local/bin/blinky

# Copy migrations (needed for runtime database migrations)
COPY --from=builder /app/migrations /app/migrations

# Set working directory
WORKDIR /app

# Use non-root user
USER blinky:blinky

# Expose port (document the port, actual port mapping done at runtime)
EXPOSE 8080

# Health check using the application's health endpoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health_check || exit 1

# Set default environment variables
ENV RUST_LOG=info \
    RUST_BACKTRACE=0

# Run the binary
ENTRYPOINT ["/usr/local/bin/blinky"]