FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV CELERY_WORKER_CONCURRENCY=10

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/data/input /app/data/output /app/data/logs

EXPOSE 8000

# Default command for FastAPI
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]