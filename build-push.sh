set -e

docker build -t brojonat/airflow:1.0.0 .
docker push brojonat/airflow:1.0.0