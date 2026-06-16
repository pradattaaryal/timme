from pydantic import BaseModel
from typing import List, Optional


class Job(BaseModel):
    title: Optional[str]
    company: Optional[str]
    location: Optional[str]
    minimum_qualifications: Optional[str] = None
    url: Optional[str] = None
    posted_date: Optional[str] = None


class JobListResponse(BaseModel):
    count: int
    jobs: List[Job]