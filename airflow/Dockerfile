# Extends the official Apache Airflow image with the packages required by the
# Healthcare DW pipeline: Snowflake connector, Great Expectations, dbt, and
# the HTTP provider for the CMS sensor.
#
# Build: docker-compose build
# Run:   docker-compose up -d
#
# For production, pin every package version and use a private registry.
# For local development, build with:
#   docker build -f airflow/Dockerfile -t healthcare-dw-airflow:latest .

FROM apache/airflow:2.9.0

# Switch to root to install OS-level dependencies if needed, then back to airflow.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        # Required by snowflake-connector for certificate validation
        ca-certificates \
        # Required by some GE backends
        libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
