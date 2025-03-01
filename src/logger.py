import logging
import sys

logger = logging.getLogger("etl_pipeline")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
