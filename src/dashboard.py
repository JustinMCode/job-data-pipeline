# dashboard.py

import streamlit as st
import psycopg2
from datetime import datetime
from typing import List, Dict
from src.utils.config import DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT
from src.utils.logger import logger

# Page configuration
st.set_page_config(
    page_title="AI Job Board",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_db_connection():
    """Create and return a PostgreSQL connection"""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

@st.cache_data(ttl=3600, show_spinner="Loading jobs...")
def fetch_jobs(filters: dict = None) -> List[Dict]:
    """Fetch jobs from PostgreSQL with optional filters"""
    base_query = """
        SELECT 
            job_title, employer_name, job_employment_type,
            job_application_link, job_description, job_is_remote,
            job_location, job_city, job_state, job_country,
            job_benefits, job_salary, job_min_salary, job_max_salary,
            job_highlights, job_responsibilities, date_posted
        FROM job_data
        WHERE 1=1
    """
    params = []
    
    # Build filter conditions
    conditions = []
    if filters:
        if filters.get("search_query"):
            conditions.append("""
                (job_title ILIKE %s OR 
                 job_description ILIKE %s OR 
                 employer_name ILIKE %s)
            """)
            params.extend([f"%{filters['search_query']}%"]*3)
            
        if filters.get("location"):
            conditions.append("(job_location ILIKE %s OR job_city ILIKE %s)")
            params.extend([f"%{filters['location']}%"]*2)
            
        # Change from exact match to pattern matching
        if filters.get("employment_type"):
            conditions.append("job_employment_type ILIKE %s")
            params.append(f"%{filters['employment_type']}%")
            
        if filters.get("remote_only"):
            conditions.append("job_is_remote = TRUE")
            
        if filters.get("min_salary"):
            conditions.append("COALESCE(job_salary, job_max_salary) >= %s")
            params.append(filters["min_salary"])

    query = base_query
    if conditions:
        query += " AND " + " AND ".join(conditions)
        
    query += " ORDER BY date_posted DESC"
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        st.error("Failed to load jobs from database")
        return []
    finally:
        if conn:
            conn.close()

def format_salary(job: dict) -> str:
    """Format salary information with proper Unicode characters"""
    salary_style = (
        "font-size: 1rem; "
        "font-weight: 500; "
        "font-family: sans-serif;"
    )
    
    if job.get('job_salary'):
        return f"<span style='{salary_style}'>${job['job_salary']:,.0f}</span>"
        
    if job.get('job_min_salary') and job.get('job_max_salary'):
        return (
            f"<span style='{salary_style}'>"
            f"${job['job_min_salary']:,.0f} – ${job['job_max_salary']:,.0f}"
            "</span>"
        )
    
    return "<span style='font-size: 1rem'>Not specified</span>"

def display_job_card(job: dict):
    """Render a single job card with expandable details"""
    def format_markdown_bullets(text: str) -> str:
        """Convert bullet points to Markdown format"""
        if not text:
            return ""
        return text.replace('•', '*').replace('\n', '  \n')
    
    with st.container():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.subheader(job["job_title"])
            st.caption(f"**{job['employer_name']}** - {job['job_location']}")
            
            if job["job_is_remote"]:
                st.markdown("🏠 **Remote Position**")
                
            st.write(f"📅 **Posted:** {job['date_posted'].strftime('%Y-%m-%d') if job['date_posted'] else 'N/A'}")
            st.write(f"💼 **Employment Type:** {job['job_employment_type'] or 'Not specified'}")
            st.markdown(
                f'<div style="margin: 5px 0;">💰 <strong>Salary:</strong> {format_salary(job)}</div>',
                unsafe_allow_html=True
            )
                        
        with col2:
            st.link_button("Apply Now", job["job_application_link"])
        
        # Collapsible details section
        # Collapsible details section
        with st.expander("View Job Details"):
            tab1, tab2, tab3 = st.tabs(["Description", "Responsibilities", "Benefits"])
            
            with tab1:
                desc = format_markdown_bullets(job["job_description"])
                st.markdown(desc or "*No description available*")
                
            with tab2:
                resp = format_markdown_bullets(job["job_responsibilities"])
                st.markdown(resp or "*No responsibilities listed*")
                
            with tab3:
                benefits = format_markdown_bullets(job["job_benefits"])
                st.markdown(benefits or "*No benefits information available*")

def main():
    """Main dashboard layout and logic"""
    st.title("💼 AI-Powered Job Board")
    st.markdown("### Curated Tech Opportunities with AI-Enhanced Listings")
    
    # Sidebar Filters
    with st.sidebar:
        st.header("🔍 Search Filters")
        
        search_query = st.text_input("Search jobs by keyword")
        location = st.text_input("Location")
        employment_type = st.selectbox(
            "Employment Type",
            ["", "Full-time", "Part-time", "Contract", "Internship"],
            format_func=lambda x: "Any" if x == "" else x
        )
        min_salary = st.number_input("Minimum Salary (USD)", min_value=0, step=10000)
        remote_only = st.checkbox("Remote Only")
        
        st.markdown("---")
        st.markdown("ℹ️ Data updated hourly using AI processing")
    
    # Apply filters
    filters = {
        "search_query": search_query,
        "location": location,
        "employment_type": employment_type,
        "min_salary": min_salary if min_salary > 0 else None,
        "remote_only": remote_only
    }
    
    # Load jobs
    jobs = fetch_jobs(filters)
    
    # Display results
    st.markdown(f"📄 **Found {len(jobs)} matching positions**")
    
    if not jobs:
        st.info("No jobs found matching your criteria. Try adjusting your filters.")
        return
    
    for job in jobs:
        display_job_card(job)
        st.markdown("---")

if __name__ == "__main__":
    main()