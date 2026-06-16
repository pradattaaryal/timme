import os
import socket

from celery import Celery
from celery.signals import worker_process_init

from src.dependencies import reset_job_service_cache
from src.infrastructure.parallel_api_pool.credentials import reset_round_robin_pool
from src.services.acquisition_service import run_monthly_acquisition
from src.telemetry.setup import configure_telemetry, ensure_propagator_configured, is_telemetry_enabled

default_redis_host = "redis"
try:
    socket.gethostbyname(default_redis_host)
    default_broker = f"redis://{default_redis_host}:6379/0"
except OSError:
    default_broker = "redis://localhost:6379/0"

broker_url = os.getenv("CELERY_BROKER_URL", default_broker)
result_backend = os.getenv("CELERY_RESULT_BACKEND", broker_url)

# Propagator only on import (API publish path); full SDK init runs in worker_process_init.
if is_telemetry_enabled():
    ensure_propagator_configured()

celery_app = Celery(
    "worker",
    broker=broker_url,
    backend=result_backend,
)


@worker_process_init.connect(weak=False)
def _init_worker_process(**_kwargs: object) -> None:
    # Fresh credential pool + job provider per forked worker (avoid inherited parent state).
    reset_round_robin_pool()
    reset_job_service_cache()
    configure_telemetry(
        service_name=os.getenv("OTEL_SERVICE_NAME_CELERY", "oxylab-celery-worker"),
        instrument_celery=True,
    )


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def run_acquisition_task(
    self,
    input_path=None,
    pages=1,
    limit_per_query=20,
    resume_from_checkpoint=True,
    retry_dead_queue=True,
):
    return run_monthly_acquisition(
        input_path=input_path,
        pages=pages,
        limit_per_query=limit_per_query,
        resume_from_checkpoint=resume_from_checkpoint,
        retry_dead_queue=retry_dead_queue,
        celery_task_id=str(self.request.id or ""),
        celery_retry_count=int(getattr(self.request, "retries", 0) or 0),
    )
