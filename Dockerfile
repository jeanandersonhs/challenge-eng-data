FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_DIRS=/tmp/spark \
    HOME=/tmp

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --no-cache-dir --editable ".[dev,notebook]"

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "eng_data_challenge.ingestion"]

