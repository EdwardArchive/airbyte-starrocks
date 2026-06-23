# From-source build of the (Python) StarRocks destination connector.
# Mirrors the upstream starrocks/destination-starrocks:1.0.0 runtime
# (python 3.11, airbyte-cdk 0.62.1, entrypoint python main.py).
FROM python:3.11-slim

WORKDIR /airbyte/integration_code

RUN pip install --no-cache-dir "poetry==1.8.3"

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
 && poetry install --no-root --only main --no-interaction --no-ansi

COPY main.py ./
COPY destination_starrocks ./destination_starrocks

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
