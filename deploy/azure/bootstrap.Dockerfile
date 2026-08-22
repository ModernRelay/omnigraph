ARG BASE_IMAGE
FROM ${BASE_IMAGE}

# The server image already ends as USER omnigraph. COPY metadata must be
# correct at creation time; a later unprivileged chmod/chown cannot repair it.
COPY --chown=omnigraph:omnigraph --chmod=0755 bootstrap-entrypoint.sh /opt/omnigraph/bootstrap-entrypoint.sh
COPY --chown=omnigraph:omnigraph bundle/ /opt/omnigraph/bundle/

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/omnigraph/bootstrap-entrypoint.sh"]
