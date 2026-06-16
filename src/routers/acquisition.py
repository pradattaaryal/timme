from fastapi import APIRouter, HTTPException

from src.celery_app import run_acquisition_task
from src.models.acquisition import AcquisitionEnqueueResponse, AcquisitionRunRequest
from src.telemetry.celery_trace import enqueue_acquisition_task

router = APIRouter(prefix="/acquisition", tags=["Acquisition"])


@router.post("/run", response_model=AcquisitionEnqueueResponse)
def run_acquisition(payload: AcquisitionRunRequest) -> AcquisitionEnqueueResponse:
    try:
        async_result = enqueue_acquisition_task(
            run_acquisition_task,
            input_path=payload.input_path,
            pages=payload.pages,
            limit_per_query=payload.limit_per_query,
            resume_from_checkpoint=payload.resume_from_checkpoint,
            retry_dead_queue=payload.retry_dead_queue,
        )
        return AcquisitionEnqueueResponse(task_id=async_result.id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Acquisition failed: {exc}")
