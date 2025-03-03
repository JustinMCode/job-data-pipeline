import pandas as pd
from io import BytesIO
from typing import List, Tuple
import logging
from datetime import datetime, timezone
from src.utils.logger import logger

REQUIRED_COLUMNS = [
    "job_title", "employer_name", "job_employment_type",
    "job_application_link", "job_description", "job_is_remote",
    "job_location", "job_city", "job_state", "job_country",
    "job_benefits", "job_salary", "job_min_salary", "job_max_salary",
    "job_highlights", "job_responsibilities", "date_posted", "job_hash"
]

def validate_columns(df: pd.DataFrame) -> bool:
    """Validate DataFrame contains all required columns"""
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        logger.error(f"Missing required columns: {', '.join(missing)}")
        return False
    return True

def process_csv_data(csv_data: bytes, chunk_size: int = 1000) -> List[Tuple]:
    """Process CSV data into database-ready tuples"""
    processed_data = []
    
    try:
        for chunk in pd.read_csv(BytesIO(csv_data), chunksize=chunk_size, dtype=str):
            if not validate_columns(chunk):
                raise ValueError("CSV validation failed")

            # Convert and clean data
            chunk['date_posted'] = pd.to_datetime(
                chunk['date_posted'], errors='coerce', utc=True
            ).dt.tz_convert(None)
            
            # Process each row
            for row in chunk[REQUIRED_COLUMNS].itertuples(index=False, name=None):
                cleaned_row = [
                    None if pd.isnull(value) else value 
                    for value in row
                ]
                # Add current timestamp
                cleaned_row += (datetime.now(timezone.utc),)
                processed_data.append(tuple(cleaned_row))
                
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error: {str(e)}")
    except Exception as e:
        logger.error(f"Data processing error: {str(e)}")
    
    return processed_data