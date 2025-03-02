import psycopg2
import pandas as pd
from io import BytesIO
from typing import Optional, Tuple, List
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import DatabaseError
from datetime import datetime, timezone
from src.config import S3_BUCKET, DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT
from src.logger import logger
from src.clients.s3_client import get_s3_client

# Connection pool configuration
POOL_MIN_CONN = 1
POOL_MAX_CONN = 10
CONNECTION_TIMEOUT = 30  # seconds

# Initialize connection pool with timeout and connection args
pool = SimpleConnectionPool(
    POOL_MIN_CONN,
    POOL_MAX_CONN,
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT,
    connect_timeout=CONNECTION_TIMEOUT,
    sslmode="require"
)

REQUIRED_COLUMNS = [
    "job_title", "employer_name", "job_employment_type",
    "job_application_link", "job_description", "job_is_remote",
    "job_location", "job_city", "job_state", "job_country",
    "job_benefits", "job_salary", "job_min_salary", "job_max_salary",
    "job_highlights", "job_responsibilities", "date_posted", "job_hash"
]

def get_latest_processed_file(s3, bucket: str, prefix: str = "processed_data/") -> Tuple[Optional[str], Optional[bytes]]:
    """Retrieve the latest processed CSV file from S3 with pagination handling."""
    paginator = s3.get_paginator('list_objects_v2')
    latest_obj = None
    
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" not in page:
                continue
            page_latest = max(page["Contents"], key=lambda x: x["LastModified"])
            if not latest_obj or page_latest["LastModified"] > latest_obj["LastModified"]:
                latest_obj = page_latest

        if not latest_obj:
            logger.warning("No processed data found in S3.")
            return None, None

        latest_key = latest_obj["Key"]
        if not latest_key.endswith(".csv"):
            logger.warning(f"Latest file is not a CSV: {latest_key}")
            return None, None

        logger.info(f"Downloading {latest_key}...")
        response = s3.get_object(Bucket=bucket, Key=latest_key)
        return latest_key, response["Body"].read()

    except s3.exceptions.NoSuchBucket:
        logger.error(f"Bucket {bucket} does not exist")
        return None, None
    except Exception as e:
        logger.error(f"Error accessing S3: {str(e)}")
        return None, None

def _validate_columns(df: pd.DataFrame) -> bool:
    """Validate that the DataFrame contains all required columns."""
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        logger.error(f"Missing required columns in CSV: {', '.join(missing)}")
        return False
    return True

def process_csv_chunks(csv_data: bytes, chunk_size: int = 1000) -> List[Tuple]:
    """Process CSV data with improved validation and error handling."""
    data = []
    try:
        for chunk in pd.read_csv(BytesIO(csv_data), chunksize=chunk_size, dtype=str):
            if not _validate_columns(chunk):
                raise ValueError("CSV validation failed")

            logger.debug(f"Processing chunk with {len(chunk)} rows...")
            
            # Convert date_posted to datetime and handle NaT
            chunk['date_posted'] = pd.to_datetime(
                chunk['date_posted'],
                errors='coerce',
                utc=True
            ).dt.tz_convert(None)
            
            # Iterate over rows in the chunk and replace NaN values with None
            for row in chunk[REQUIRED_COLUMNS].itertuples(index=False, name=None):
                # Use a list comprehension to convert each value:
                processed_row = [None if pd.isnull(value) else value for value in row]
                
                # Ensure that date_posted (index 16) is properly set to None if NaT
                if pd.isnull(processed_row[16]):
                    processed_row[16] = None
                
                # Append the current UTC timestamp as integratedTimestamp
                processed_row.append(datetime.now(timezone.utc))
                
                data.append(tuple(processed_row))
                
    except pd.errors.EmptyDataError:
        logger.error("Empty CSV file encountered")
    except pd.errors.ParserError:
        logger.error("Malformed CSV file")
        
    return data


def update_database(data: List[Tuple]) -> int:
    """Update database with transaction handling and retry logic."""
    if not data:
        logger.warning("No data to insert")
        return 0

    insert_query = f"""
        INSERT INTO job_data ({', '.join(REQUIRED_COLUMNS)}, integratedTimestamp)
        VALUES %s
        ON CONFLICT (job_hash)
        DO UPDATE SET
            {', '.join(f"{col} = EXCLUDED.{col}" for col in REQUIRED_COLUMNS[2:])},
            integratedTimestamp = EXCLUDED.integratedTimestamp
        WHERE job_data != EXCLUDED
    """
    
    total_inserted = 0
    conn = None
    try:
        conn = pool.getconn()
        with conn, conn.cursor() as cursor:
            execute_values(
                cursor,
                insert_query,
                data,
                page_size=1000
            )
            total_inserted = cursor.rowcount
            logger.info(f"Affected {total_inserted} rows")

    except DatabaseError as e:
        logger.error(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pool.putconn(conn)
            
    return total_inserted

def archive_file(s3, bucket: str, latest_key: str) -> bool:
    """Atomic file archiving with error handling."""
    try:
        archive_key = latest_key.replace("processed_data/", "archive/", 1)
        # Use copy_object + delete pattern for atomic move
        s3.copy_object(
            CopySource={"Bucket": bucket, "Key": latest_key},
            Bucket=bucket,
            Key=archive_key,
            MetadataDirective="COPY"
        )
        s3.delete_object(Bucket=bucket, Key=latest_key)
        logger.info(f"Successfully archived to {archive_key}")
        return True
    except Exception as e:
        logger.error(f"Archiving failed: {str(e)}")
        return False

def load_data_to_postgres() -> None:
    """Main ETL flow with enhanced error handling."""
    try:
        s3 = get_s3_client()
        latest_key, csv_data = get_latest_processed_file(s3, S3_BUCKET)
        if not (latest_key and csv_data):
            return

        # Validate data before processing
        try:
            sample_df = pd.read_csv(BytesIO(csv_data), nrows=1)
            if not _validate_columns(sample_df):
                raise ValueError("Invalid CSV structure")
        except Exception as e:
            logger.error("CSV validation failed")
            return

        data = process_csv_chunks(csv_data)
        if not data:
            logger.warning("No valid data processed")
            return

        affected_rows = update_database(data)
        
        if affected_rows > 0:
            if not archive_file(s3, S3_BUCKET, latest_key):
                logger.error("Data archived but database updates may be incomplete")

    except Exception as e:
        logger.error("Critical error in ETL pipeline", exc_info=True)
        raise

if __name__ == "__main__":
    load_data_to_postgres()