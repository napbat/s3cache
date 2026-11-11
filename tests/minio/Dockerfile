# syntax=docker/dockerfile:1
# This image is only an S3 origin for tests. Build from MinIO's official source.
FROM golang:1-bookworm AS build

ARG MINIO_COMMIT=7aac2a2c5b7c882e68c1ce017d8256be2feea27f
WORKDIR /src
RUN git init . \
 && git remote add origin https://github.com/minio/minio.git \
 && git fetch --depth 1 origin "${MINIO_COMMIT}" \
 && git checkout --detach FETCH_HEAD \
 && test "$(git rev-parse HEAD)" = "${MINIO_COMMIT}"

# Bound compiler parallelism to limit memory use on a developer host.
RUN GOMAXPROCS=2 CGO_ENABLED=0 go build -p 2 -tags kqueue -trimpath -o /out/minio .

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir /data \
 && chown 10001:10001 /data
COPY --from=build /out/minio /usr/local/bin/minio
ENV HOME=/tmp
USER 10001:10001
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/minio"]
