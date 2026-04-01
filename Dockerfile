FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY target/release/omnigraph-server /usr/local/bin/omnigraph-server

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/omnigraph-server"]
