FROM apache/airflow:2.5.0

USER airflow
# Install latest compatible versions
RUN pip install --no-cache-dir \
    apache-airflow-providers-oracle \
    cassandra-driver \
    oracledb