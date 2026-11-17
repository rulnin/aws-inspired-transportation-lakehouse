FROM apache/airflow:slim-3.4.0-python3.12@sha256:2d3e42b9de0eca948b9a869817068fedcb8a8eac9827b1e6215de3329d59335f

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow[statsd]==3.4.0" \
    "apache-airflow-providers-docker==4.6.1" \
    "psycopg2-binary==2.9.10" \
    "asyncpg==0.31.0" \
    "boto3==1.44.3" \
    "pyarrow==24.0.0"
