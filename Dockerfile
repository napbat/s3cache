# syntax=docker/dockerfile:1
# rust-toolchain.toml selects `stable`, and the workspace's rust-version is the
# latest stable. The `rust:1` image can lag a new stable release by days, so
# install the toolchain that file selects instead of trusting the image's.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY rust-toolchain.toml ./
RUN rustup toolchain install
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked && cp target/release/s3cache /usr/local/bin/s3cache

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/s3cache /usr/local/bin/s3cache
# S3 API port (S3CACHE_LISTEN overrides).
EXPOSE 8014
ENTRYPOINT ["/usr/local/bin/s3cache"]
