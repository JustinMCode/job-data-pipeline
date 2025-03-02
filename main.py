# main.py
from src.jobs.fetch_jobs import main_fetch
from src.jobs.process_jobs import process_jobs
from src.jobs.load_to_postgresql import load_data_to_postgres
from src.logger import logger

def main(): 
    try:
        main_fetch()
        process_jobs()
        load_data_to_postgres()
    except Exception as e:
        logger.error("ETL pipeline failed at some step.", exc_info=True)

if __name__ == "__main__":
    main()
