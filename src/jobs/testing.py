import json
import re

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

# Example usage:
if __name__ == "__main__":
    # This is an example API response (with code block markers)
    example_response = '''```json
{
    "Job Description": "Snap Inc is seeking a Data Engineer to join their Core Growth Team. The company focuses on leveraging the camera to enhance communication and daily life through products like Snapchat, Lens Studio, and Spectacles.",
    "Qualifications Needed": [
        "Experience in building data pipelines for reporting needs",
        "Ownership of team roadmaps",
        "Ability to prioritize requests from various stakeholders",
        "Effective communication of complex projects to non-technical stakeholders",
        "BS/BA degree in Computer Science, Math, Physics, or related field, or equivalent experience",
        "5+ years of experience in SQL or similar languages",
        "5+ years of development experience in object-oriented or scripting languages (Python, Java, Scala, etc)"
    ],
    "Job Responsibilities": [
        "Work closely with stakeholders to provide high-quality datasets in a timely manner",
        "Develop scalable data ETL pipelines to automate processes and ensure data privacy",
        "Implement and manage data warehousing solutions with rigorous testing",
        "Build tools to enhance data consumption portals",
        "Maintain data security practices for privacy and compliance"
    ],
    "Job Benefits": [
        "Comprehensive health insurance coverage",
        "Paid parental leave",
        "Emotional and mental health support programs",
        "Compensation packages tied to Snap's long-term success"
    ]
}
```'''
    
    parsed_info = parse_simplified_job_info(example_response)
    print("Parsed Job Description:")
    print(parsed_info["job_description"])
    print("\nParsed Qualifications Needed:")
    print(parsed_info["qualifications_needed"])
    print("\nParsed Job Responsibilities:")
    print(parsed_info["job_responsibilities"])
    print("\nParsed Job Benefits:")
    print(parsed_info["job_benefits"])
