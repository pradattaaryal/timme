import os

from fastapi import FastAPI

from src.routers.acquisition import router as acquisition_router
from src.routers.health import router as health_router
from src.routers.jobs import router as jobs_router
from src.telemetry.setup import configure_telemetry, instrument_fastapi, is_telemetry_enabled

if is_telemetry_enabled():
    configure_telemetry(
        service_name=os.getenv("OTEL_SERVICE_NAME_API", "oxylab-api"),
        instrument_celery=True,
    )

app = FastAPI(
    title="Job Crawler API",
    description="FastAPI service for fetching job listings via SerpAPI, Oxylabs, or env-configured providers.",
    version="1.0.0",
)

instrument_fastapi(app)

app.include_router(health_router)
app.include_router(jobs_router)
app.include_router(acquisition_router)
