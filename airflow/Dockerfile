FROM apache/airflow:2.9.3-python3.11

USER airflow

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --timeout 120 \
    "apache-airflow==2.9.3" \
    -r /tmp/requirements.txt

RUN mkdir -p /opt/airflow/state /opt/airflow/data-lake

COPY --chown=airflow:root dags/ /opt/airflow/dags/