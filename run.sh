curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.5.0/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
AIRFLOW_UID=50000
docker compose up airflow-init
docker-compose up
chrome http://localhost:8080
docker exec -it airflow_airflow-webserver_1 bash
    cat airflow.cfg
docker run --name postgress -p 5432:5432 -e POSTGRES_PASSWORD=root -e PGPASSWORD=root -e POSTGRES_USER=root -d ananthhh/postgress
docker run -it --rm postgres psql -h 192.168.1.12 -U root
curl -L http://192.168.1.10/housing.csv -o housing.csv