# src/openai_processor.py

import json
import re
import openai
from openai import AsyncOpenAI
import hashlib
import logging
from typing import Optional
from src.config import OPENAI_KEY, OPENAI_MODEL

# Configure OpenAI client
client = AsyncOpenAI(api_key=OPENAI_KEY)

# Set up logging
logger = logging.getLogger(__name__)

# Cache configuration
CACHE_SIZE_LIMIT = 1000  # Prevent unlimited memory growth
simplification_cache = {}

def _create_cache_key(prompt_template: str, text: str, **kwargs) -> str:
    """Create unique cache key considering all relevant parameters."""
    key_data = f"{prompt_template}{text}{kwargs}"
    return hashlib.sha256(key_data.encode("utf-8")).hexdigest()

async def simplify_text(
    prompt_template: str,
    text: str,
    max_tokens: int = 1000,
    temperature: float = 0.5,
    model: str = OPENAI_MODEL,
    retries: int = 3
) -> dict:
    """
    Simplify text using the provided prompt template with ChatCompletion.
    Implements caching, retries, and proper error handling. After receiving
    the response from the API, it parses the output into its four header parts.

    Args:
        prompt_template: Template with <<INSERT JOB TEXT HERE>> placeholder
        text: Content to process
        max_tokens: Response length limit
        temperature: Model creativity control
        model: OpenAI model to use
        retries: Number of retry attempts

    Returns:
        A dictionary with keys:
            - job_description
            - qualifications_needed
            - job_responsibilities
            - job_benefits
        or the original text wrapped in a dict on error.
    """
    if not text.strip():
        print("Empty text received; returning as is.")
        return {"job_description": text, "qualifications_needed": "", "job_responsibilities": "", "job_benefits": ""}
    
    cache_key = _create_cache_key(prompt_template, text, max_tokens=max_tokens, temperature=temperature)
    
    if cache_key in simplification_cache:
        print("Cache hit for simplified text")
        cached_output = simplification_cache[cache_key]
        parsed = parse_simplified_job_info(cached_output)
        return parsed

    full_prompt = prompt_template.replace("<<INSERT JOB TEXT HERE>>", text)
    # print("Full prompt being sent to the model:")
    # print(full_prompt)
    
    simplified_text = text  # Fallback value

    for attempt in range(retries):
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant specialized in summarizing job information.",
                    },
                    {"role": "user", "content": full_prompt},
                ],
                max_tokens=max_tokens,
                temperature=temperature,
            )
            simplified_text = response.choices[0].message.content.strip()
            print(f"Response received on attempt {attempt+1}:")
            
            # Update cache and enforce size limit
            if len(simplification_cache) >= CACHE_SIZE_LIMIT:
                simplification_cache.popitem()
            simplification_cache[cache_key] = simplified_text
            
            print(f"Successfully generated simplified text on attempt {attempt+1}")
            break
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {str(e)}")
            if attempt == retries - 1:
                print("All retries exhausted, returning original text")
    
    # Integrate parsing: convert the raw JSON output into a dictionary with the 4 headers.
    try:
        parsed_output = parse_simplified_job_info(simplified_text)
        return parsed_output
    except Exception as e:
        print(f"Parsing failed: {str(e)}. Returning raw simplified text.")
        #return {"job_description": simplified_text, "qualifications_needed": "", "job_responsibilities": "", "job_benefits": ""}


async def simplify_job_info(job_data_json_output: str) -> str: 
    """
    Simplify information about the job description, requirements, qualifications, and benefits
    
    Argss: 
        job_data_json_output: Json of the job description, requirements, qualifications, and benefits

    Returns: 
        Concise summary of all job info
    """
    prompt = (
         "Given the following job information:\n\n"
        f"{job_data_json_output}\n\n"
        "1. Job Description: Provide a concise summary tailored to the job.\n"
        "2. Qualifications Needed: Present clear bullet points, list core skills and qualifications.\n"
        "3. Job Responsibilities: Present clear bullet points for the main tasks.\n"
        "4. Job Benefits: Present clear bullet points, list potential benefits (using general examples if necessary).\n\n"
        "Format your answer using these section headings exactly as shown and convert it to a json object:\n"
        "- **Job Description:**\n"
        "- **Qualifications Needed:**\n"
        "- **Job Responsibilities:**\n"
        "- **Job Benefits:**"
    )
    return await simplify_text(prompt, job_data_json_output)


# Testing 
def parse_simplified_job_info(api_response: str) -> dict:
    """
    Parse the API output containing simplified job information.
    Extracts the four header sections and returns them as strings.
    
    For the list-based sections, returns a bullet list (one bullet per line).
    
    Args:
        api_response (str): The raw JSON string from the API output,
                            which may be wrapped in code block markers.
    
    Returns:
        dict: A dictionary with the following keys:
            - job_description
            - qualifications_needed
            - job_responsibilities
            - job_benefits
        Each value is a string.
    """
    # Remove any code block markers (```json and ```)
    cleaned_response = re.sub(r"```(json)?", "", api_response).strip()
    cleaned_response = re.sub(r"```", "", cleaned_response).strip()
    
    try:
        data = json.loads(cleaned_response)
    except json.JSONDecodeError as e:
        raise ValueError("Failed to parse API response as valid JSON") from e

    # Extract the Job Description
    job_description = data.get("Job Description", "").strip()
    
    # For list values, create a bullet list (each item on a new line prefixed by a bullet marker)
    def make_bullet_list(items):
        if isinstance(items, list):
            # Remove any extraneous spaces and create a bullet point for each item
            return "\n".join(f"• {item.strip()}" for item in items)
        else:
            return str(items).strip()
    
    qualifications_needed = make_bullet_list(data.get("Qualifications Needed", []))
    job_responsibilities = make_bullet_list(data.get("Job Responsibilities", []))
    job_benefits = make_bullet_list(data.get("Job Benefits", []))

    return {
        "job_description": job_description,
        "qualifications_needed": qualifications_needed,
        "job_responsibilities": job_responsibilities,
        "job_benefits": job_benefits
    }
