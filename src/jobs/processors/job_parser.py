import json
from typing import Dict, Any
from src.ai.openai_processor import simplify_job_info

async def parse_job_data(raw_job: Dict[str, Any]) -> Dict[str, Any]:
    """Parse job data using OpenAI processor"""
    job_data = {
        "job_description": raw_job.get("job_description", ""),
        "job_highlights": raw_job.get("job_highlights", {}),
        "job_requirements": " ".join(raw_job.get("responsibilities", [])),
        "job_benefits": raw_job.get("job_benefits")
    }
    
    return await simplify_job_info(json.dumps(job_data, indent=4))