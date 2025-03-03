from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class ProcessedJob:
    job_title: str
    employer_name: str
    job_employment_type: str
    job_application_link: str
    job_description: str
    job_is_remote: bool
    job_location: str
    job_city: str
    job_state: str
    job_country: str
    job_benefits: Optional[str]
    job_salary: Optional[float]
    job_min_salary: Optional[float]
    job_max_salary: Optional[float]
    job_highlights: Optional[str]
    job_responsibilities: Optional[str]
    date_posted: Optional[datetime]
    job_hash: str
    integrated_timestamp: datetime