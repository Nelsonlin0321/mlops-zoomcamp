#!/bin/bash
mlflow server --backend-store-uri ${MLFLOW_BACKEND_STORE_URI} --default-artifact-root s3://${MLFLOW_S3_ENDPOINT_URL}/mlflow --host 0.0.0.0 --port ${PORT}
