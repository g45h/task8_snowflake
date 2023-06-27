To start the project:

mkdir -p ./logs ./plugins ./config ./data

add data file to the ./data directory

echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init

docker-compose up -d
_______________
create a connection: Airflow -> Admin -> Connections -> New Connection:

Connection Id: file_sensor

Connection Type: fs

Extra: {"path": "/opt/airflow/data"}