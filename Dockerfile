FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent_kafka

COPY producer.py consumer.py ./
