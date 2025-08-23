# Use Airflow with Python 3.9
FROM apache/airflow:2.7.1-python3.9

# Copy requirements into image
COPY requirements.txt /requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Ensure Airflow directories exist with correct ownership
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins 
