from datetime import datetime
from dateutil.parser import parse
from typing import Dict, Any
from src.utils.data_utils import validate_url, clean_salary

def clean_job_data(raw_job: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and validate raw job data"""
    cleaned = raw_job.copy()
    
    # URL validation
    cleaned['job_application_link'] = (
        raw_job['job_apply_link'] 
        if validate_url(raw_job.get('job_apply_link', '')) 
        else 'INVALID_URL'
    )
    
    # Date parsing
    cleaned['date_posted'] = None
    for date_field in ['job_posted_at_datetime_utc', 'date_posted']:
        if date_str := raw_job.get(date_field):
            try:
                cleaned['date_posted'] = parse(date_str, fuzzy=True)
                break
            except Exception:
                continue
                
    # Salary cleaning
    cleaned.update({
        'job_salary': clean_salary(str(raw_job.get('job_salary', ''))),
        'job_min_salary': clean_salary(str(raw_job.get('job_min_salary', ''))),
        'job_max_salary': clean_salary(str(raw_job.get('job_max_salary', ''))),
    })
    
    return cleaned