FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir pyspark==3.5.1 influxdb-client

COPY processor.py /app/processor.py

CMD ["python", "/app/processor.py"]