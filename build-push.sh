set -e

docker build -t brojonat/airflow:v1.0 .
docker push brojonat/airflow:v1.0