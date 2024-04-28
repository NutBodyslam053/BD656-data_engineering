FROM apache/airflow:slim-2.8.2-python3.11

COPY requirements.txt /opt/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/requirements.txt