import psycopg2
import pandas as pd
from io import BytesIO
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime
from src.config import S3_BUCKET, DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT
from src.logger import logger
from src.clients.s3_client import get_s3_client

# Initialize a connection pool (min 1, max 10)
pool = SimpleConnectionPool(
    1, 10,
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)

def get_latest_processed_file(s3, bucket: str, prefix: str = "processed_data/") -> tuple[str, bytes]:
    """
    Retrieve the latest processed CSV file from S3.

    Returns:
        A tuple containing the latest key and the file content as bytes.
        If no file is found, returns (None, None).
    """
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response or not response["Contents"]:
        logger.warning("No processed data found in S3.")
        return None, None
    latest_obj = max(response["Contents"], key=lambda x: x["LastModified"])
    latest_key = latest_obj["Key"]
    if not latest_key.endswith(".csv"):
        logger.warning(f"Latest file is not a CSV: {latest_key}")
        return None, None
    logger.info(f"Downloading {latest_key}...")
    raw_obj = s3.get_object(Bucket=bucket, Key=latest_key)
    return latest_key, raw_obj["Body"].read()

def process_csv_chunks(csv_data: bytes, chunk_size: int = 1000) -> list:
    """
    Process CSV data in chunks, appending the current UTC timestamp to each row.

    Returns:
        A list of tuples representing rows with an appended integratedTimestamp.
    """
    required_cols = [
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
    data = []
    for chunk in pd.read_csv(BytesIO(csv_data), chunksize=chunk_size):
        logger.info(f"Processing chunk with {len(chunk)} rows...")
        # Convert date_posted to datetime
        chunk['date_posted'] = pd.to_datetime(chunk['date_posted'], errors='coerce')
        for row in chunk[required_cols].itertuples(index=False, name=None):
            row = list(row)
            # Replace invalid dates (NaN) with None
            if pd.isnull(row[16]):
                row[16] = None
            # Append the current UTC timestamp for integratedTimestamp
            row.append(datetime.utcnow())
            data.append(tuple(row))
    return data

def update_database(data: list):
    """
    Update the PostgreSQL database with the provided data using a bulk operation.
    
    The query uses an ON CONFLICT clause to update the record if any field has changed.
    """
    insert_query = """
        INSERT INTO job_data (
            job_title,
            employer_name,
            job_employment_type,
            job_application_link,
            job_description,
            job_is_remote,
            job_location,
            job_city,
            job_state,
            job_country,
            job_benefits,
            job_salary,
            job_min_salary,
            job_max_salary,
            job_highlights,
            job_responsibilities,
            date_posted,
            job_hash,
            integratedTimestamp
        )
        VALUES %s
        ON CONFLICT (job_hash)
        DO UPDATE SET
            job_employment_type = EXCLUDED.job_employment_type,
            job_application_link = EXCLUDED.job_application_link,
            job_description = EXCLUDED.job_description,
            job_is_remote = EXCLUDED.job_is_remote,
            job_location = EXCLUDED.job_location,
            job_city = EXCLUDED.job_city,
            job_state = EXCLUDED.job_state,
            job_country = EXCLUDED.job_country,
            job_benefits = EXCLUDED.job_benefits,
            job_salary = EXCLUDED.job_salary,
            job_min_salary = EXCLUDED.job_min_salary,
            job_max_salary = EXCLUDED.job_max_salary,
            job_highlights = EXCLUDED.job_highlights,
            job_responsibilities = EXCLUDED.job_responsibilities,
            date_posted = EXCLUDED.date_posted,
            integratedTimestamp = EXCLUDED.integratedTimestamp
        WHERE
            job_data.job_employment_type IS DISTINCT FROM EXCLUDED.job_employment_type OR
            job_data.job_application_link IS DISTINCT FROM EXCLUDED.job_application_link OR
            job_data.job_description IS DISTINCT FROM EXCLUDED.job_description OR
            job_data.job_is_remote IS DISTINCT FROM EXCLUDED.job_is_remote OR
            job_data.job_location IS DISTINCT FROM EXCLUDED.job_location OR
            job_data.job_city IS DISTINCT FROM EXCLUDED.job_city OR
            job_data.job_state IS DISTINCT FROM EXCLUDED.job_state OR
            job_data.job_country IS DISTINCT FROM EXCLUDED.job_country OR
            job_data.job_benefits IS DISTINCT FROM EXCLUDED.job_benefits OR
            job_data.job_salary IS DISTINCT FROM EXCLUDED.job_salary OR
            job_data.job_min_salary IS DISTINCT FROM EXCLUDED.job_min_salary OR
            job_data.job_max_salary IS DISTINCT FROM EXCLUDED.job_max_salary OR
            job_data.job_highlights IS DISTINCT FROM EXCLUDED.job_highlights OR
            job_data.job_responsibilities IS DISTINCT FROM EXCLUDED.job_responsibilities OR
            job_data.date_posted IS DISTINCT FROM EXCLUDED.date_posted
    """
    conn = pool.getconn()
    total_inserted = 0
    try:
        with conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, data)
                total_inserted = len(data)
                logger.info(f"Inserted/Updated {total_inserted} rows.")
        logger.info(f"Data successfully loaded into PostgreSQL. Total rows inserted/updated: {total_inserted}")
    finally:
        pool.putconn(conn)

def archive_file(s3, bucket: str, latest_key: str):
    """
    Archive the processed file by copying it to the archive folder and deleting the original.
    """
    archive_key = latest_key.replace("processed_data/", "archive/")
    s3.copy_object(
        CopySource={"Bucket": bucket, "Key": latest_key},
        Bucket=bucket,
        Key=archive_key
    )
    s3.delete_object(Bucket=bucket, Key=latest_key)
    logger.info(f"File archived to s3://{bucket}/{archive_key} and removed from processed_data.")

def load_data_to_postgres():
    """
    Main function to load processed CSV data from S3 into PostgreSQL.
    """
    s3 = get_s3_client()
    try:
        latest_key, csv_data = get_latest_processed_file(s3, S3_BUCKET)
        if not latest_key or not csv_data:
            return

        # Optional: Log the row count by loading CSV into a DataFrame
        df = pd.read_csv(BytesIO(csv_data))
        logger.info(f"DataFrame loaded with {len(df)} rows.")

        # Process CSV data in chunks and add integratedTimestamp
        data = process_csv_chunks(csv_data)

        # Update the PostgreSQL database
        update_database(data)

        # Archive the processed file in S3
        archive_file(s3, S3_BUCKET, latest_key)

    except Exception as e:
        logger.error("Error while loading data to PostgreSQL.", exc_info=True)

if __name__ == "__main__":
    load_data_to_postgres()
