# process_jobs.py

import json
import re
import pandas as pd
from io import BytesIO
from datetime import datetime
from dateutil.parser import parse
import hashlib
import asyncio
from urllib.parse import urlparse
from dataclasses import dataclass, asdict
from typing import Optional, List
from more_itertools import chunked

from src.config import S3_BUCKET
from src.logger import logger
from src.clients.s3_client import get_s3_client
from src.ai.openai_processor import simplify_job_info

# Constants
MAX_CONCURRENT_TASKS = 50  # Limit concurrent OpenAI API calls
CSV_BATCH_SIZE = 1000      # Number of records per CSV buffer flush

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

def clean_salary(salary_str: str) -> Optional[float]:
    """Handle international number formats and currency symbols"""
    if not salary_str:
        return None
    
    try:
        # Remove commas used as thousand separators and currency symbols
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

async def process_job_async(job: dict, semaphore: asyncio.Semaphore) -> Optional[ProcessedJob]:
    """Process a single job with concurrency control and error handling"""
    async with semaphore:
        try:
            # Parse and validate URL
            raw_link = job.get("job_apply_link", "")
            job_application_link = raw_link if validate_url(raw_link) else "INVALID_URL"

            # Date parsing with multiple fallbacks
            date_posted = None
            for date_field in ["job_posted_at_datetime_utc", "date_posted"]:
                if date_str := job.get(date_field):
                    try:
                        date_posted = parse(date_str, fuzzy=True)
                        break
                    except Exception as e:
                        logger.debug(f"Failed to parse {date_field}: {date_str} - {str(e)}")

            # Salary cleaning
            salaries = {
                "job_salary": clean_salary(str(job.get("job_salary", ""))),
                "job_min_salary": clean_salary(str(job.get("job_min_salary", ""))),
                "job_max_salary": clean_salary(str(job.get("job_max_salary", ""))),
            }

            # Process job highlights
            job_highlights_obj = job.get("job_highlights", {})
            responsibilities = []
            if job_highlights_obj:
                responsibilities = job_highlights_obj.pop("Responsibilities", [])            
            # Create base job object
            base_job = {
                "job_title": job.get("job_title", ""),
                "employer_name": job.get("employer_name", ""),
                "job_employment_type": job.get("job_employment_type", ""),
                "job_application_link": job_application_link,
                "job_is_remote": job.get("job_is_remote", False),
                "job_location": job.get("job_location", ""),
                "job_city": job.get("job_city", ""),
                "job_state": job.get("job_state", ""),
                "job_country": job.get("job_country", ""),
                "date_posted": date_posted,
                **salaries,
                "job_hash": job.get("job_id") or generate_job_hash(job)
            }

            job_data = {
                "job_description": job.get("job_description", ""),
                "job_highlights": job_highlights_obj,  
                "job_requirements": " ".join(responsibilities),  
                "job_benefits": job.get("job_benefits")
            }

            job_data_json_output = json.dumps(job_data, indent=4)

            # Parallel processing of text fields: gather returns a list of results
            parsed_job_info_list = await asyncio.gather(simplify_job_info(job_data_json_output))
            parsed_job_info = parsed_job_info_list[0]  # Extract the dictionary from the list

            return ProcessedJob(
                **base_job,
                job_description=parsed_job_info["job_description"],
                job_highlights=parsed_job_info["qualifications_needed"],
                job_responsibilities=parsed_job_info["job_responsibilities"],
                job_benefits=parsed_job_info["job_benefits"]
            )

        except Exception as e:
            job_id = job.get("job_id", "unknown")
            logger.error(f"Failed to process job {job_id}: {str(e)}", exc_info=True)
            return None

async def process_job_batch(jobs: List[dict]) -> List[ProcessedJob]:
    """Process a batch of jobs with concurrency control"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    tasks = [process_job_async(job, semaphore) for job in jobs]
    results = await asyncio.gather(*tasks)
    return [job for job in results if job is not None]

async def process_and_upload(s3, jobs_data: List[dict], key: str):
    """Process jobs and upload to S3 in batches"""
    processed_jobs = []
    
    for batch in chunked(jobs_data, CSV_BATCH_SIZE):
        batch_result = await process_job_batch(batch)
        processed_jobs.extend(batch_result)
        
        # Convert to DataFrame
        df = pd.DataFrame([asdict(job) for job in batch_result])
        
        # Validate before upload
        if not df.empty:
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False, escapechar="\\", quoting=1)
            csv_buffer.seek(0)
            
            upload_key = key.replace("raw_data/", "processed_data/").replace(".json", f"_{len(processed_jobs)}.csv")
            s3.put_object(Bucket=S3_BUCKET, Key=upload_key, Body=csv_buffer.getvalue())
            logger.info(f"Uploaded batch {upload_key} with {len(df)} records")

    logger.info(f"Total processed jobs: {len(processed_jobs)}/{len(jobs_data)}")

async def fetch_raw_data(s3, key: str) -> List[dict]:
    """Fetch and validate raw data from S3"""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        raw_data = json.loads(response["Body"].read())
        
        if not isinstance(raw_data.get("data"), list):
            raise ValueError("Invalid data format: expected list in 'data' field")
            
        return raw_data["data"]
    except Exception as e:
        logger.error(f"Failed to fetch/parse raw data from {key}: {str(e)}")
        raise

async def main_async(s3_client=None):
    """Async main workflow"""
    s3 = s3_client or get_s3_client()
    
    try:
        # Find latest raw data file
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="raw_data/")
        if not response.get("Contents"):
            logger.warning("No raw data files found")
            return

        latest_obj = max(response["Contents"], key=lambda x: x["LastModified"])
        latest_key = latest_obj["Key"]
        logger.info(f"Processing latest data file: {latest_key}")

        # Process and upload
        jobs_data = await fetch_raw_data(s3, latest_key)
        await process_and_upload(s3, jobs_data, latest_key)

    except Exception as e:
        logger.error(f"Critical error in processing pipeline: {str(e)}", exc_info=True)
        raise

def process_jobs(s3_client=None):
    """Entry point with proper async handling"""
    try:
        asyncio.run(main_async(s3_client))
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error in main process: {str(e)}", exc_info=True)

if __name__ == "__main__":
    process_jobs()