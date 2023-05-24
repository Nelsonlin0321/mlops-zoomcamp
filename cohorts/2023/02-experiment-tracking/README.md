docker build -t mlflow-postgres-with-s3:latest .

docker run -d -p 5050:5050 --env-file docker.env mlflow-postgres-with-s3:latest
