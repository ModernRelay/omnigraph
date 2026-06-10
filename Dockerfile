# Pulled from ECR Public (the Debian team mirrors official images there)
# instead of Docker Hub to avoid anonymous-pull rate limits from AWS
# CodeBuild, which shares an outbound IP pool with many other accounts.
FROM public.ecr.aws/debian/debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system omnigraph \
    && useradd --system --gid omnigraph --create-home --home-dir /var/lib/omnigraph omnigraph

COPY target/release/omnigraph-server /usr/local/bin/omnigraph-server
# The CLI ships in the image so the cluster day-2 loop (cluster
# apply/approve/status, data loads by explicit URI) runs in-container via
# `docker exec` / ECS exec / `railway shell` — no omnigraph.yaml required.
COPY target/release/omnigraph /usr/local/bin/omnigraph
COPY docker/entrypoint.sh /usr/local/bin/omnigraph-entrypoint

RUN chmod 0755 /usr/local/bin/omnigraph-server /usr/local/bin/omnigraph /usr/local/bin/omnigraph-entrypoint

ENV OMNIGRAPH_BIND=0.0.0.0:8080

WORKDIR /var/lib/omnigraph
USER omnigraph:omnigraph

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/healthz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/omnigraph-entrypoint"]
