import psycopg2
from psycopg2.extras import execute_values
from typing import List, Tuple, Optional
from src.clients.s3_client import get_s3_client
from src.clients.postgres_client import PostgresClient
from src.jobs.loaders.s3_loader import get_latest_processed_file, archive_file
from src.jobs.processors.data_processor import process_csv_data, REQUIRED_COLUMNS
from src.utils.config import S3_BUCKET
from src.utils.logger import logger

def update_database(data: List[Tuple]) -> int:
    """Update database with processed data"""
    if not data:
        logger.warning("No data to insert")
        return 0

    # Change the INSERT query to match the exact column name:
    insert_query = f"""
        INSERT INTO job_data ({', '.join(REQUIRED_COLUMNS)}, integrated_timestamp)
        VALUES %s
        ON CONFLICT (job_hash)
        DO UPDATE SET
            {', '.join(f"{col} = EXCLUDED.{col}" for col in REQUIRED_COLUMNS[2:])},
            integrated_timestamp = NOW()
    """

    conn = None
    try:
        conn = PostgresClient.get_connection()
        with conn, conn.cursor() as cursor:
            execute_values(
                cursor,
                insert_query,
                data,
                page_size=1000
            )
            affected_rows = cursor.rowcount
            logger.info(f"Successfully upserted {affected_rows} records")
            return affected_rows
            
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        if conn:
            PostgresClient.release_connection(conn)

def load_data_to_postgres() -> None:
    """Main ETL orchestration function"""
    try:
        s3 = get_s3_client()
        
        # Get latest processed file
        file_key, csv_data = get_latest_processed_file(s3)
        if not csv_data:
            return

        # Process data
        processed_data = process_csv_data(csv_data)
        if not processed_data:
            logger.warning("No valid data processed")
            return

        # Update database
        affected_rows = update_database(processed_data)
        
        # Archive file if successful
        if affected_rows > 0:
            if not archive_file(s3, file_key):
                logger.error("Failed to archive processed file")

    except Exception as e:
        logger.error("ETL pipeline failed", exc_info=True)
        raise

if __name__ == "__main__":
    load_data_to_postgres()