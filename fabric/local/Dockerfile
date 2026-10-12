# Python 3.11, Spark 3.5 and Delta 3.2 match the versions used by Fabric Runtime 1.3.
FROM python:3.11-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.3 pyarrow==16.1.0
