ARG SUPERSET_IMAGE_TAG=latest
FROM apache/superset:${SUPERSET_IMAGE_TAG}

USER root
RUN /app/.venv/bin/python -m ensurepip --upgrade \
    && /app/.venv/bin/python -m pip install --no-cache-dir clickhouse-connect psycopg2-binary
USER superset
