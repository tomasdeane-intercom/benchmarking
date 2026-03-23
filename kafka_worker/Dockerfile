FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY benchkit /app/benchkit
COPY canonical /app/canonical

RUN pip install .

CMD ["python", "-m", "benchkit.cli.kafka_worker"]
