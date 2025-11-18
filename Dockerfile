# Stage 1: Build Rust binary
FROM rustlang/rust:nightly AS builder
WORKDIR /usr/src/rust_redis

# Copy Cargo files first for caching
COPY Cargo.toml Cargo.lock ./

# Create empty src folder for caching
RUN mkdir src

# Pre-build dependencies (cache layer)
RUN cargo build --release || true

# Copy source code and benches
COPY ./src ./src
COPY ./benches ./benches

# Build release binary
RUN cargo build --release

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

WORKDIR /usr/local/bin
COPY --from=builder /usr/src/rust_redis/target/release/rust_redis .

EXPOSE 6380
CMD ["./rust_redis"]
