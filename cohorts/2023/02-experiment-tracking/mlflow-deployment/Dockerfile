FROM python:3.8-slim-buster

# Install dependencies
RUN apt-get update && \
    apt-get install gcc -y && \
    apt-get clean

# Install MLflow with S3 dependency
ENV MLFLOW_VERSION=2.3.2
RUN pip install mlflow==$MLFLOW_VERSION boto3 psycopg2-binary

# Expose port for MLflow UIcke
EXPOSE ${PORT}

COPY boot.sh .
# Start MLflow server
ENTRYPOINT ["sh","./boot.sh"]