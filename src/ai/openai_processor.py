# src/openai_processor.py

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
    max_tokens: int = 150,
    temperature: float = 0.5,
    model: str = OPENAI_MODEL,
    retries: int = 3
) -> str:
    """
    Simplify text using the provided prompt template with ChatCompletion.
    Implements caching, retries, and proper error handling.

    Args:
        prompt_template: Template with <<INSERT JOB TEXT HERE>> placeholder
        text: Content to process
        max_tokens: Response length limit
        temperature: Model creativity control
        model: OpenAI model to use
        retries: Number of retry attempts

    Returns:
        Simplified text or original text on error
    """
    if not text.strip():
        return text

    cache_key = _create_cache_key(prompt_template, text, 
                                 max_tokens=max_tokens, temperature=temperature)
    
    if cache_key in simplification_cache:
        logger.debug("Cache hit for simplified text")
        return simplification_cache[cache_key]

    full_prompt = prompt_template.replace("<<INSERT JOB TEXT HERE>>", text)
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
            
            # Update cache and enforce size limit
            if len(simplification_cache) >= CACHE_SIZE_LIMIT:
                simplification_cache.popitem()
            simplification_cache[cache_key] = simplified_text
            
            logger.info(f"Successfully generated simplified text (attempt {attempt+1})")
            break
        except Exception as e:
            logger.warning(
                f"Attempt {attempt+1} failed: {str(e)}",
                exc_info=attempt == retries-1
            )
            if attempt == retries - 1:
                logger.error("All retries exhausted, returning original text")
    
    return simplified_text

async def simplify_job_description(description: str) -> str:
    """
    Simplify a job description by focusing on key responsibilities and requirements.
    
    Args:
        description: Full job description text
        
    Returns:
        Concise summary starting with 'Job Description:'
    """
    prompt = (
        "Please simplify the following job description. "
        "Reword it to provide a concise summary that includes the key responsibilities, "
        "required skills, and any essential details about the role. "
        "Remove extraneous language and focus on the most important information.\n\n"
        "Start the new text with Job Description:\n<<INSERT JOB TEXT HERE>>"
    )
    return await simplify_text(prompt, description)

async def simplify_job_highlights(highlights: str) -> str:
    """
    Simplify job highlights to core qualifications and key skills.
    
    Args:
        highlights: Job highlights text
        
    Returns:
        Simplified list starting with 'Qualifications Needed:'
    """
    prompt = (
        "Please rephrase the following job highlights, focusing on the core qualifications "
        "and key skills required for the role. Simplify the content by removing any redundant "
        "details while preserving all essential information.\n\n"
        "Start the new text with Qualifications Needed:\n<<INSERT JOB TEXT HERE>>"
    )
    return await simplify_text(prompt, highlights)

async def simplify_job_responsibilities(responsibilities: str) -> str:
    """
    Simplify job responsibilities into clear, concise bullet points.
    
    Args:
        responsibilities: List of job responsibilities
        
    Returns:
        Cleaned bullet points starting with 'Job Responsibilities:'
    """
    prompt = (
        "Please simplify the following list of job responsibilities. "
        "Reword the content into clear and concise bullet points that capture "
        "the core tasks and expectations of the role. "
        "Eliminate any unnecessary or repetitive details.\n\n"
        "Start the new text with Job Responsibilities:\n<<INSERT JOB TEXT HERE>>"
    )
    return await simplify_text(prompt, responsibilities)