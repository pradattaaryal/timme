import requests
from fastapi import APIRouter, Depends, HTTPException, Query

from src.application.ports.job_provider import FetchJobParams
from src.application.services.job_service import JobService
from src.config.settings import settings
from src.dependencies import get_job_service
from src.models.job import JobListResponse

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.get("", response_model=JobListResponse)
def get_jobs(
    query: str = Query(default=settings.DEFAULT_QUERY),
    location: str = Query(default=settings.DEFAULT_LOCATION),
    limit: int = Query(default=10, ge=1, le=50),
    enrich: bool = Query(
        default=False,
        description="Fetch provider pages (e.g. Indeed) for title, job type, and other fields.",
    ),
    job_service: JobService = Depends(get_job_service),
):
    try:
        jobs = job_service.get_jobs(
            FetchJobParams(
                limit=limit, query=query, location=location, enrich=enrich
            )
        )
        return JobListResponse(count=len(jobs), jobs=jobs)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response else 502
        upstream_text = ""
        if exc.response is not None:
            upstream_text = (exc.response.text or exc.response.reason or "").strip()[:300]
        raise HTTPException(
            status_code=status_code,
            detail=f"Upstream job API error ({status_code}): {upstream_text or str(exc)}"
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to fetch jobs: {exc}"
        )