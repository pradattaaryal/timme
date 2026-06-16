from pydantic import BaseModel, Field


class AcquisitionRunRequest(BaseModel):
    input_path: str | None = Field(default=None, description="Path to input CSV or Excel")
    pages: int = Field(default=1, ge=1, le=2, description="Number of pages to retrieve per query")
    limit_per_query: int = Field(default=1, ge=1, le=100, description="Maximum jobs to extract per query")
    resume_from_checkpoint: bool = Field(
        default=False,
        description="Resume unfinished processing from the latest checkpoint if available",
    )
    retry_dead_queue: bool = Field(
        default=False,
        description="Retry records from dead queue before processing remaining records",
    )


class AcquisitionEnqueueResponse(BaseModel):
    task_id: str


class AcquisitionRunResponse(BaseModel):
    input_path: str
    output_csv: str
    summary_excel: str
    validation_log: str
    error_log: str
    total_input_records: int
    valid_records: int
    skipped_records: int
    success_rate: float
    success_records: int
    no_result_records: int
    error_records: int
    api_requests: int
