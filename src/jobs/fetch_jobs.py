import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import requests
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    before_sleep_log
)
from requests.exceptions import RequestException, HTTPError
from src.config import RAPIDAPI_KEY, S3_BUCKET, RAPIDAPI_HOST, API_REQUEST_TIMEOUT
from src.logger import logger
from src.clients.s3_client import get_s3_client

# Constants
API_BASE_URL = "https://jsearch.p.rapidapi.com/search"
DEFAULT_QUERY_PARAMS = {
    "query": "Data Engineer",
    "location": "USA",
    "page": 1,
    "num_pages": 2,
    "country": "us",
    "date_posted": "all"
}
S3_RAW_DATA_PREFIX = "raw_data/"
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

def _get_api_headers() -> Dict[str, str]:
    """Return standardized API headers."""
    return {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    retry=(
        retry_if_exception_type(RequestException) |
        retry_if_exception_type(HTTPError)
    ),
    before_sleep=before_sleep_log(logger, logging.WARNING),  
    reraise=True
)

def fetch_jobs(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Fetch job data from RapidAPI endpoint with enhanced error handling and retries.
    
    Args:
        params: Dictionary of query parameters to override defaults
        
    Returns:
        Dictionary containing API response data
        
    Raises:
        HTTPError: For 4xx/5xx status codes after retries exhausted
        RequestException: For network-related errors
    """
    try:
        logger.info("Initiating job data fetch")
        final_params = DEFAULT_QUERY_PARAMS.copy()
        if params:
            final_params.update(params)

        with requests.Session() as session:
            response = session.get(
                API_BASE_URL,
                headers=_get_api_headers(),
                params=final_params,
                timeout=API_REQUEST_TIMEOUT
            )
            
            response.raise_for_status()
            
            logger.debug(f"API response received - Status: {response.status_code}")
            return response.json()

    except HTTPError as e:
        if e.response.status_code in RETRY_STATUS_CODES:
            logger.warning(f"Retryable error: {str(e)}")
            raise
        logger.error(f"Non-retryable HTTP error: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error("Failed to parse API response JSON")
        raise ValueError("Invalid JSON response") from e

def upload_to_s3(data: Dict[str, Any], bucket: str) -> str:
    """Upload data to S3 with validation and error handling."""
    if not bucket:
        raise ValueError("S3 bucket name is required")

    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_name = f"jobs_{timestamp}.json"
        s3_key = f"{S3_RAW_DATA_PREFIX}{file_name}"
        data_bytes = json.dumps(data).encode("utf-8")

        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=data_bytes,
            ContentType="application/json"
        )

        logger.info(f"Successfully uploaded to s3://{bucket}/{s3_key}")
        return s3_key
    except Exception as e:
        logger.error("S3 upload failed", exc_info=True)
        raise

def main_fetch() -> None:
    """Orchestrate job fetching and data upload workflow."""
    try:
        if not all([RAPIDAPI_KEY, RAPIDAPI_HOST, S3_BUCKET]):
            raise EnvironmentError("Missing required environment variables")

        # Fetch data
        job_data = fetch_jobs()
        logger.info(f"Received {len(job_data.get('data', []))} job listings")

        # Upload to S3
        s3_key = upload_to_s3(job_data, S3_BUCKET)
        
        # Optional: Add subsequent processing steps here
        return s3_key

    except Exception as e:
        logger.error("Job fetch pipeline failed", exc_info=True)
        raise

if __name__ == "__main__":
    main_fetch()