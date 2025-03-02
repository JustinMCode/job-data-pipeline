# src/openai_processor.py

import openai
import hashlib
import asyncio
import logging
from src.config import OPENAI_KEY

# Configure OpenAI API key
openai.api_key = OPENAI_KEY

# Set up logging
logger = logging.getLogger(__name__)

# In-memory cache to avoid duplicate API calls
simplification_cache = {}

async def simplify_text(prompt_template: str, text: str, max_tokens: int = 150, temperature: float = 0.5) -> str:
    """
    Simplify text using the provided prompt template with ChatCompletion.
    Uses caching to avoid duplicate API calls.
    """
    # Create a unique cache key based on the prompt and text
    cache_key = hashlib.sha256((prompt_template + text).encode("utf-8")).hexdigest()
    if cache_key in simplification_cache:
        logger.info("Cache hit for simplified text.")
        return simplification_cache[cache_key]

    # Replace placeholder in the prompt with the actual text
    full_prompt = prompt_template.replace("<<INSERT JOB TEXT HERE>>", text)

    try:
        # Call the ChatCompletion API asynchronously
        response = await openai.ChatCompletion.acreate(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant specialized in summarizing job information."},
                {"role": "user", "content": full_prompt}
            ],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        simplified_text = response.choices[0].message["content"].strip()
        simplification_cache[cache_key] = simplified_text
        logger.info("Simplified text successfully generated and cached.")
        return simplified_text
    except Exception as e:
        logger.error(f"Error simplifying text: {e}", exc_info=True)
        # Fallback: return the original text if API call fails
        return text

async def simplify_job_description(description: str) -> str:
    prompt = (
        "Please simplify the following job description. "
        "Reword it to provide a concise summary that includes the key responsibilities, required skills, and any essential details about the role. "
        "Remove extraneous language and focus on the most important information.\n\n"
        "Start the new text with Job Description:\n<<INSERT JOB TEXT HERE>>"
    )
    return await simplify_text(prompt, description)

async def simplify_job_highlights(highlights: str) -> str:
    prompt = (
        "Please rephrase the following job highlights, focusing on the core qualifications and key skills required for the role. "
        "Simplify the content by removing any redundant details while preserving all essential information.\n\n"
        "Start the new text with Qualifications Needed:\n<<INSERT JOB TEXT HERE>>"
    )
    return await simplify_text(prompt, highlights)

async def simplify_job_responsibilities(responsibilities: str) -> str:
    prompt = (
        "Please simplify the following list of job responsibilities. "
        "Reword the content into clear and concise bullet points that capture the core tasks and expectations of the role. "
        "Eliminate any unnecessary or repetitive details.\n\n"
        "Start the new text with Job Responsibilities:\n<<INSERT JOB TEXT HERE>>"
    )
    return await simplify_text(prompt, responsibilities)
