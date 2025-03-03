import re
from typing import Optional
from urllib.parse import urlparse
import hashlib

def clean_salary(salary_str: str) -> Optional[float]:
    """Handle international number formats and currency symbols"""
    if not salary_str:
        return None
    try:
        cleaned = re.sub(r"[^\d.]", "", salary_str.replace(",", ""))
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None

def validate_url(link: str) -> bool:
    """Validate URL format"""
    try:
        result = urlparse(link)
        return all([result.scheme, result.netloc])
    except:
        return False

def generate_job_hash(job: dict) -> str:
    """Generate consistent hash from job data"""
    hash_input = (
        f"{job.get('job_title', '').strip().lower()}|"
        f"{job.get('employer_name', '').strip().lower()}|"
        f"{job.get('job_location', '').strip().lower()}|"
        f"{job.get('job_posted_at_datetime_utc', '')}|"
        f"{job.get('job_apply_link', '').strip().lower()}"
    )
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()