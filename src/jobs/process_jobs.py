# src/jobs/process_jobs.py

import asyncio
from datetime import datetime, timezone
import json
from io import BytesIO
from typing import List, Optional, Dict, Any
import pandas as pd
from more_itertools import chunked

from src.models.job_models import ProcessedJob
from src.jobs.processors.job_cleaner import clean_job_data
from src.jobs.processors.job_parser import parse_job_data
from src.utils.data_utils import generate_job_hash
from src.clients.s3_client import get_s3_client
from src.utils.config import S3_BUCKET
from src.utils.logger import logger

# Constants
MAX_CONCURRENT_TASKS = 50  # Limit concurrent OpenAI API calls
CSV_BATCH_SIZE = 1000      # Number of records per CSV buffer flush

async def process_job_async(
    raw_job: dict, 
    semaphore: asyncio.Semaphore
) -> Optional[ProcessedJob]:
    """Process a single job with concurrency control and error handling"""
    async with semaphore:
        try:
            # Step 1: Clean raw job data
            cleaned_job = clean_job_data(raw_job)
            
            # Step 2: Parse job data with AI processing
            parsed_data = await parse_job_data(cleaned_job)
            
            # Step 3: Create processed job object
            return ProcessedJob(
                job_title=cleaned_job.get("job_title", ""),
                employer_name=cleaned_job.get("employer_name", ""),
                job_employment_type=cleaned_job.get("job_employment_type", ""),
                job_application_link=cleaned_job.get("job_application_link", ""),
                job_is_remote=cleaned_job.get("job_is_remote", False),
                job_location=cleaned_job.get("job_location", ""),
                job_city=cleaned_job.get("job_city", ""),
                job_state=cleaned_job.get("job_state", ""),
                job_country=cleaned_job.get("job_country", ""),
                date_posted=cleaned_job.get("date_posted"),
                job_salary=cleaned_job.get("job_salary"),
                job_min_salary=cleaned_job.get("job_min_salary"),
                job_max_salary=cleaned_job.get("job_max_salary"),
                job_hash=generate_job_hash(raw_job),
                job_description=parsed_data.get("job_description", ""),
                job_highlights=parsed_data.get("qualifications_needed", ""),
                job_responsibilities=parsed_data.get("job_responsibilities", ""),
                job_benefits=parsed_data.get("job_benefits", ""),
                integrated_timestamp=datetime.now(timezone.utc)
            )
        except Exception as e:
            job_id = raw_job.get("job_id", "unknown")
            logger.error(f"Failed to process job {job_id}: {str(e)}", exc_info=True)
            return None

async def process_job_batch(jobs: List[dict]) -> List[ProcessedJob]:
    """Process a batch of jobs with concurrency control"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    tasks = [process_job_async(job, semaphore) for job in jobs]
    results = await asyncio.gather(*tasks)
    return [job for job in results if job is not None]

async def process_and_upload(s3_client, jobs_data: List[dict], key: str) -> None:
    """Process jobs and upload to S3 in batches"""
    processed_jobs = []
    
    for batch in chunked(jobs_data, CSV_BATCH_SIZE):
        batch_result = await process_job_batch(batch)
        processed_jobs.extend(batch_result)
        
        if batch_result:
            # Convert to DataFrame and validate
            df = pd.DataFrame([job.__dict__ for job in batch_result])
            
            # Prepare CSV buffer
            csv_buffer = BytesIO()
            df.to_csv(
                csv_buffer,
                index=False,
                encoding="utf-8",
                escapechar="\\",
                quoting=1
            )
            csv_buffer.seek(0)
            
            # Generate upload key
            upload_key = key.replace("raw_data/", "processed_data/")
            upload_key = upload_key.replace(".json", f"_{len(processed_jobs)}.csv")
            
            # Upload to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=upload_key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv"
            )
            logger.info(f"Uploaded batch {upload_key} with {len(df)} records")

    logger.info(f"Processing complete. Total jobs: {len(processed_jobs)}/{len(jobs_data)}")

async def fetch_raw_data(s3_client, key: str) -> List[dict]:
    """Fetch and validate raw data from S3"""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        raw_data = json.loads(response["Body"].read())
        
        if not isinstance(raw_data.get("data"), list):
            raise ValueError("Invalid data format: expected list in 'data' field")
            
        return raw_data["data"]
    except Exception as e:
        logger.error(f"Failed to fetch/parse raw data from {key}: {str(e)}")
        raise

async def main_async(s3_client=None) -> None:
    """Async main processing workflow"""
    s3 = s3_client or get_s3_client()
    
    try:
        # Find latest raw data file
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="raw_data/")
        if not response.get("Contents"):
            logger.warning("No raw data files found")
            return

        # Get most recent file
        latest_obj = max(response["Contents"], key=lambda x: x["LastModified"])
        latest_key = latest_obj["Key"]
        logger.info(f"Processing latest data file: {latest_key}")

        # Process and upload
        jobs_data = await fetch_raw_data(s3, latest_key)
        await process_and_upload(s3, jobs_data, latest_key)

    except Exception as e:
        logger.error(f"Critical error in processing pipeline: {str(e)}", exc_info=True)
        raise

def process_jobs(s3_client=None) -> None:
    """Entry point with proper async handling"""
    try:
        asyncio.run(main_async(s3_client))
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error in main process: {str(e)}", exc_info=True)

if __name__ == "__main__":
    process_jobs()