import requests
import json
from datetime import datetime
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from requests.exceptions import RequestException
from src.config import RAPIDAPI_KEY, S3_BUCKET
from src.logger import logger
from src.clients.s3_client import get_s3_client

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5),
       retry=retry_if_exception_type(RequestException))
def fetch_jobs():
    """
    Fetch job data from the RapidAPI endpoint with automatic retries for transient errors.
    """
    base_url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": "Data Engineer",
        "location": "USA",
        "page": 1,
        "num_pages": 1,
        "country": "us",
        "date_posted": "all"
    }
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }
    
    logger.info("Starting job fetch process...")
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()  # Will raise HTTPError if not 200-299
    logger.info("Received successful response.")
    
    return response.json()

def main_fetch():
    try:
        job_data = fetch_jobs()
        
        # Print the complete JSON response to the console for inspection.
        print(json.dumps(job_data, indent=2))
        
        s3 = get_s3_client()
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_name = f"jobs_{timestamp}.json"
        s3_key = f"raw_data/{file_name}"
        data_bytes = json.dumps(job_data).encode("utf-8")
        logger.info(f"Uploading file to S3: s3://{S3_BUCKET}/{s3_key}")
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=data_bytes)
        logger.info(f"✅ Successfully uploaded to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error("❌ An error occurred while fetching jobs.", exc_info=True)

if __name__ == "__main__":
    main_fetch()
