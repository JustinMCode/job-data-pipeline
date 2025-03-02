import json
import re
import pandas as pd
from io import BytesIO
from datetime import datetime
from dateutil.parser import parse  
from src.config import S3_BUCKET
from src.logger import logger
from src.s3_client import get_s3_client
import hashlib

def clean_salary(salary_str):
    """
    Remove non-numeric characters (except the decimal point) and convert to float.
    """
    if not salary_str:
        return None
    cleaned = re.sub(r"[^\d\.]", "", salary_str)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None

def generate_job_hash(job_title, employer_name, job_location, date_posted, job_application_link):
    """
    Generate a SHA-256 hash based on key job fields.
    """
    title_norm = job_title.strip().lower() if job_title else ""
    employer_norm = employer_name.strip().lower() if employer_name else ""
    location_norm = job_location.strip().lower() if job_location else ""
    date_norm = date_posted.isoformat() if date_posted else ""
    link_norm = job_application_link.strip().lower() if job_application_link else ""
    hash_input = f"{title_norm}|{employer_norm}|{location_norm}|{date_norm}|{link_norm}"
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

def process_jobs():
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="raw_data/")
        if "Contents" not in response or len(response["Contents"]) == 0:
            logger.warning("No raw data found in S3.")
            return

        latest_obj = max(response["Contents"], key=lambda x: x["LastModified"])
        latest_key = latest_obj["Key"]

        logger.info(f"Downloading {latest_key}...")
        raw_obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_key)
        raw_data = json.loads(raw_obj["Body"].read())

        if "data" not in raw_data:
            logger.warning(f"No 'data' field in JSON for key {latest_key}. Skipping.")
            return

        jobs_list = []
        for job in raw_data["data"]:
            # Parse date_posted
            date_posted_str = job.get("job_posted_at_datetime_utc", "")
            try:
                date_posted = parse(date_posted_str, fuzzy=True) if date_posted_str else None
            except Exception:
                logger.warning(f"Failed to parse date '{date_posted_str}', setting as None.")
                date_posted = None

            # Clean salary fields
            raw_min_salary = job.get("job_min_salary", "")
            raw_max_salary = job.get("job_max_salary", "")
            job_min_salary = clean_salary(str(raw_min_salary))
            job_max_salary = clean_salary(str(raw_max_salary))
            job_salary = job.get("job_salary", None)
            if job_salary is not None:
                try:
                    job_salary = float(job_salary)
                except:
                    job_salary = None

            # Extract other fields
            job_title = job.get("job_title", "")
            employer_name = job.get("employer_name", "")
            job_employment_type = job.get("job_employment_type", "")
            job_application_link = job.get("job_apply_link", "")
            job_description = job.get("job_description", "")
            job_is_remote = job.get("job_is_remote", False)
            job_location = job.get("job_location", "")
            job_city = job.get("job_city", "")
            job_state = job.get("job_state", "")
            job_country = job.get("job_country", "")
            job_benefits = job.get("job_benefits", "")

            # Process job_highlights and extract responsibilities
            job_highlights_obj = job.get("job_highlights", {})
            if job_highlights_obj:
                # Remove responsibilities so they're not included in job_highlights
                responsibilities_list = job_highlights_obj.pop("Responsibilities", [])
                job_responsibilities = ", ".join(responsibilities_list) if responsibilities_list else ""
                job_highlights = json.dumps(job_highlights_obj) if job_highlights_obj else ""
            else:
                job_highlights = ""
                job_responsibilities = ""

            # Use job_id as unique identifier if available; otherwise, generate a hash.
            job_id = job.get("job_id", "")
            if job_id:
                job_hash = job_id
            else:
                job_hash = generate_job_hash(job_title, employer_name, job_location, date_posted, job_application_link)

            jobs_list.append({
                "job_title": job_title,
                "employer_name": employer_name,
                "job_employment_type": job_employment_type,
                "job_application_link": job_application_link,
                "job_description": job_description,
                "job_is_remote": job_is_remote,
                "job_location": job_location,
                "job_city": job_city,
                "job_state": job_state,
                "job_country": job_country,
                "job_benefits": job_benefits,
                "job_salary": job_salary,
                "job_min_salary": job_min_salary,
                "job_max_salary": job_max_salary,
                "job_highlights": job_highlights,
                "job_responsibilities": job_responsibilities,
                "date_posted": date_posted,
                "job_hash": job_hash
            })

        # Define the explicit column order
        columns = [
            "job_title",
            "employer_name",
            "job_employment_type",
            "job_application_link",
            "job_description",
            "job_is_remote",
            "job_location",
            "job_city",
            "job_state",
            "job_country",
            "job_benefits",
            "job_salary",
            "job_min_salary",
            "job_max_salary",
            "job_highlights",
            "job_responsibilities",
            "date_posted",
            "job_hash"
        ]
        df = pd.DataFrame(jobs_list, columns=columns)
        df.drop_duplicates(subset=["job_hash"], inplace=True)

        csv_buffer = BytesIO()
        # Write CSV using an empty string for NaN values
        df.to_csv(csv_buffer, index=False, na_rep="")
        csv_buffer.seek(0)

        processed_key = latest_key.replace("raw_data/", "processed_data/").replace(".json", ".csv")
        s3.put_object(Bucket=S3_BUCKET, Key=processed_key, Body=csv_buffer.getvalue())
        logger.info(f"Processed data uploaded to s3://{S3_BUCKET}/{processed_key}")

    except Exception as e:
        logger.error("An error occurred while processing jobs.", exc_info=True)

if __name__ == "__main__":
    process_jobs()
