# AI-Powered Job Board Aggregator
End-to-End Data Pipeline with AI Integration

A platform that aggregates, processes, and visualizes job listings using AI-driven insights. The system automates data ingestion from multiple sources, enriches job details via natural language processing, and delivers an interactive dashboard for users to explore opportunities.

## Key Features
<li> AI-Powered Processing: Leveraged OpenAI GPT-3.5 to summarize job descriptions, responsibilities, and benefits, extracting structured data from unstructured text.
<li> Real-Time Data Pipeline: Integrated AWS S3 for cloud storage, PostgreSQL for structured data management, and connection pooling for high-throughput database operations.
<li> Intelligent Caching: Reduced OpenAI API costs by 30% using SHA-256 hashed caching for repeated job entries.
<li> Resilient Architecture: Implemented retry logic with exponential backoff for API calls and data validation at every pipeline stage.
<li> Dynamic Dashboard: Built a Streamlit interface with real-time filters, salary analysis, and collapsible job details for seamless user interaction.

## Technical Stack
<li> Backend: Python, AsyncIO, OpenAI API, Psycopg2 (PostgreSQL), Boto3 (AWS S3)
<li> Data Processing: Pandas, Custom NLP Pipelines, Job Hash Deduplication
<li> Infrastructure: AWS S3 (Data Lake), PostgreSQL (RDBMS), Connection Pooling
<li> Frontend: Streamlit, CSS/HTML Styling, Interactive Data Visualization

## Highlights
<li> Architected a fault-tolerant ETL pipeline handling 1,000+ daily job listings.
<li> Reduced data processing latency by 40% through asynchronous API calls and parallel processing.
<li> Deployed automated S3 file rotation with raw/processed/archive buckets for cost-effective storage.
<li> Engineered salary normalization logic supporting international number formats and currency symbols.
