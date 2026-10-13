FROM python:3.12-slim-bookworm

# Java para o Spark
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --editable ".[dev,notebook]"

# O Spark precisa de um usuário no /etc/passwd. Usar o mesmo UID de quem executa o projeto
# faz os arquivos gerados em data/ e output/ ficarem com o dono certo (make build passa o UID).
ARG UID=1000
RUN useradd --create-home --uid ${UID} app
USER app

CMD ["python", "-m", "eng_data_challenge.main"]
