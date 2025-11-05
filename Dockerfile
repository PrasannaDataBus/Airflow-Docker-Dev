FROM apache/airflow:2.9.3-python3.10

# Optional: system build deps for some wheels
USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Install Python deps for your DAGs/ETL code
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
