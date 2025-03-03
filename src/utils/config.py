import os
from dotenv import load_dotenv

load_dotenv()

import os

# RapidAPI configuration
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "your_default_rapidapi_key")
RAPIDAPI_HOST = "jsearch.p.rapidapi.com"
API_REQUEST_TIMEOUT = 15  # seconds

# AWS S3 configuration
S3_BUCKET = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

# PostgreSQL configuration
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

# OpenAI configuration
OPENAI_KEY = os.getenv("OPENAI_KEY")
OPENAI_MODEL = "gpt-3.5-turbo"

POOL_MIN_CONN = 1
POOL_MAX_CONN = 10
CONNECTION_TIMEOUT = 30