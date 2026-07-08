FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY sql/ ./sql/
COPY data/ ./data/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Same image is used for: local `docker compose run pipeline`,
# and Azure Container Apps Job (see infra/main.bicep)
ENTRYPOINT ["python", "-m", "src.pipeline"]
