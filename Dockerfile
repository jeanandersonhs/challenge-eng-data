FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_DIRS=/tmp/spark \
    HOME=/tmp

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless tini procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 app 2>/dev/null || true \
    && useradd -u 1000 -g 1000 -d /tmp -s /bin/bash app 2>/dev/null || true \
    && chmod 666 /etc/passwd /etc/group \
    && mkdir -p /tmp/spark \
    && chmod 777 /tmp/spark

WORKDIR /app

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --no-cache-dir --editable ".[dev,notebook]"

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["python", "-m", "eng_data_challenge.main"]

