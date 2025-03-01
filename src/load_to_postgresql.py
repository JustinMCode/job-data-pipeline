import psycopg2
import pandas as pd
from io import BytesIO
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
from src.config import S3_BUCKET, DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT
from src.logger import logger
from src.s3_client import get_s3_client

# Initialize a connection pool (min 1, max 10)
pool = SimpleConnectionPool(
    1, 10,
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)

def load_data_to_postgres():
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="processed_data/")
        if "Contents" not in response or len(response["Contents"]) == 0:
            logger.warning("No processed data found in S3.")
            return

        latest_obj = max(response["Contents"], key=lambda x: x["LastModified"])
        latest_key = latest_obj["Key"]
        if not latest_key.endswith(".csv"):
            logger.warning(f"Latest file is not a CSV: {latest_key}")
            return

        logger.info(f"Downloading {latest_key}...")
        raw_obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_key)
        csv_data = raw_obj["Body"].read()

        # Load CSV to a DataFrame
        df = pd.read_csv(BytesIO(csv_data))
        logger.info(f"DataFrame loaded with {len(df)} rows.")

        # Set up chunk processing parameters
        chunk_size = 1000
        total_inserted = 0

        conn = pool.getconn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    # Update required columns to match the new fields
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
                    for chunk in pd.read_csv(BytesIO(csv_data), chunksize=chunk_size):
                        logger.info(f"Processing chunk with {len(chunk)} rows...")
                        chunk['date_posted'] = pd.to_datetime(chunk['date_posted'], errors='coerce')
                        data = []
                        for row in chunk[required_cols].itertuples(index=False, name=None):
                            row = list(row)
                            # Replace invalid date with None
                            if pd.isnull(row[16]):
                                row[16] = None
                            data.append(tuple(row))
                        
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
                                job_hash
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
                                date_posted = EXCLUDED.date_posted
                        """
                        execute_values(cursor, insert_query, data)
                        total_inserted += len(data)
                        logger.info(f"Inserted {len(data)} rows in this chunk.")
            logger.info(f"Data successfully loaded into PostgreSQL. Total rows inserted: {total_inserted}")
        finally:
            pool.putconn(conn)

        archive_key = latest_key.replace("processed_data/", "archive/")
        s3.copy_object(
            CopySource={"Bucket": S3_BUCKET, "Key": latest_key},
            Bucket=S3_BUCKET,
            Key=archive_key
        )
        s3.delete_object(Bucket=S3_BUCKET, Key=latest_key)
        logger.info(f"File archived to s3://{S3_BUCKET}/{archive_key} and removed from processed_data.")

    except Exception as e:
        logger.error("Error while loading data to PostgreSQL.", exc_info=True)

if __name__ == "__main__":
    load_data_to_postgres()
