FROM apache/airflow:2.6.2
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" praw
