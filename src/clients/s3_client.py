import boto3
from src.utils.config import AWS_ACCESS_KEY, AWS_SECRET_KEY

def get_s3_client():
    """
    Returns a boto3 S3 client using credentials from configuration.
    """
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
