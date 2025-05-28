FROM apache/airflow:latest

USER airflow

# Install latest compatible versions
RUN pip install --no-cache-dir \
    apache-airflow-providers-oracle \
	cassandra-driver \
    oracledb \
	cx_Oracle 

RUN pip install --no-cache-dir --no-deps \
	apache-airflow-providers-apache-cassandra