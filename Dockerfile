FROM apache/airflow:2.9.3-python3.10

USER root

# Install system dependencies
# Added 'git' because dbt requires it for dependency checks (dbt debug)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python deps for your DAGs/ETL code
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt