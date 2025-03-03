from typing import Optional, Tuple
import logging
from src.utils.logger import logger
from src.utils.config import S3_BUCKET

def get_latest_processed_file(s3_client, prefix: str = "processed_data/") -> Tuple[Optional[str], Optional[bytes]]:
    """Retrieve the latest processed CSV file from S3"""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        latest_obj = None
        
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
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
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=latest_key)
        return latest_key, response["Body"].read()

    except Exception as e:
        logger.error(f"Error accessing S3: {str(e)}")
        return None, None

def archive_file(s3_client, latest_key: str) -> bool:
    """Archive processed file in S3"""
    try:
        archive_key = latest_key.replace("processed_data/", "archive/", 1)
        s3_client.copy_object(
            CopySource={"Bucket": S3_BUCKET, "Key": latest_key},
            Bucket=S3_BUCKET,
            Key=archive_key,
            MetadataDirective="COPY"
        )
        s3_client.delete_object(Bucket=S3_BUCKET, Key=latest_key)
        logger.info(f"Successfully archived to {archive_key}")
        return True
    except Exception as e:
        logger.error(f"Archiving failed: {str(e)}")
        return False