# Stage 1: Build static musl binary
FROM rust:alpine as builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static zstd-static zstd-dev

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src

ENV RUSTFLAGS="-C target-feature=+crt-static"
RUN cargo build --release --target x86_64-unknown-linux-musl || cargo build --release

# Stage 2: Ultra-minimal scratch container
FROM scratch

# CA certificates for TLS S3/database connections
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy statically-linked dumper executable
COPY --from=builder /build/target/*/release/dumper /dumper

# Run as unprivileged non-root user
USER 65532:65532

ENTRYPOINT ["/dumper"]
