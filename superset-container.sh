docker run -d -p 8088:8088 -e "SUPERSET_SECRET_KEY=$(openssl rand -base64 42)" -e "TALISMAN_ENABLED=False" --name superset apache/superset:3.1.0

docker exec -it superset superset fab create-admin  --username admin  --firstname Admin  --lastname Admin  --email admin@localhost  --password admin

docker exec -it superset superset db upgrade

docker exec -it superset superset init

docker exec -it superset /bin/bash

echo "clickhouse-connect>=0.6.8" >> ./requirements/local.txt

cat requirements/local.txt

pip install -r ./requirements/local.txt