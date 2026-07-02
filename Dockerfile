# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release && cp target/release/s3cache /usr/local/bin/s3cache

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/s3cache /usr/local/bin/s3cache
# S3 API port (S3CACHE_LISTEN overrides).
EXPOSE 8014
ENTRYPOINT ["/usr/local/bin/s3cache"]
