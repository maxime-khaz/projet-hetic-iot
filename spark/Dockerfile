FROM spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

WORKDIR /app

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir --timeout 120 --retries 5 -r requirements.txt

RUN mkdir -p /checkpoints /tmp/spark-ivy \
    && chown -R 185:0 /app /checkpoints /tmp/spark-ivy

COPY --chown=185:0 jobs/ /app/jobs/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3

USER 185

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--driver-memory", "1g", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "--conf", "spark.jars.ivy=/tmp/spark-ivy", \
     "/app/jobs/streaming.py"]