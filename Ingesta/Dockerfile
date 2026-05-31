FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir kafka-python websocket-client

COPY producer.py /app/producer.py

CMD ["python", "/app/producer.py"]