#!/usr/bin/env python
# coding: utf-8

import os
import sys
import uuid
import boto3
from datetime import datetime
import pandas as pd
import mlflow
from prefect import task, flow, get_run_logger
from prefect.context import get_run_context
from dateutil.relativedelta import relativedelta
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import make_pipeline
import boto3

import dotenv
dotenv.load_dotenv(".env")

logged_model  = "s3://s3-mlflow-artifacts-storage/mlflow/12/{RUN_ID}/artifacts/model"
bucket_name = 'nyc-duration-prediction'
# run_id='cc36795ca2fd48e8a176194f450c0ade'

def get_paths(run_date, taxi_type, run_id):
    prev_month = run_date - relativedelta(months=1)
    year = prev_month.year
    month = prev_month.month 
    input_file =f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'taxi_type={taxi_type}/year={year:04d}/month={month:02d}/{run_id}.parquet'
    os.makedirs(os.path.dirname(output_file),exist_ok=True)
    return input_file, output_file

@task
def upload_file_to_s3(file_name,bucket_name,object_key):
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket_name, object_key)


@task
def preprocess_dataframe(filename: str):
    df = pd.read_parquet(filename)

    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)]
    df['ride_id'] = df.apply(lambda x:str(uuid.uuid4()),axis=1)
    return df

@task
def prepare_dictionaries(df: pd.DataFrame):
    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    
    df['PU_DO'] = df['PULocationID'] + '_' + df['DOLocationID']

    categorical = ['PU_DO']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')
    return dicts

@task
def load_model(run_id):
    global logged_model
    logged_model = logged_model.format(RUN_ID=run_id)
    model = mlflow.pyfunc.load_model(logged_model)
    return model

@task
def make_prediction(model,dicts):
    y_pred = model.predict(dicts)
    return y_pred

@task
def save_results(df, y_pred, run_id, output_file):
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['lpep_pickup_datetime'] = df['lpep_pickup_datetime']
    df_result['PULocationID'] = df['PULocationID']
    df_result['DOLocationID'] = df['DOLocationID']
    df_result['actual_duration'] = df['duration']
    df_result['predicted_duration'] = y_pred
    df_result['diff'] = df_result['actual_duration'] - df_result['predicted_duration']
    df_result['model_version'] = run_id
    df_result.to_parquet(output_file, index=False)



# @flow(log_prints=True)
# def apply_model(input_file, run_id, output_file):
#     logger = get_run_logger()

#     logger.info(f'reading the data from {input_file}...')
#     df = preprocess_dataframe(input_file)
#     dicts = prepare_dictionaries(df)

#     logger.info(f'loading the model with RUN_ID={run_id}...')
#     model = load_model(run_id)

#     logger.info(f'applying the model...')
#     y_pred = make_prediction(model,dicts)

#     logger.info(f'saving the result to {output_file}...')

#     save_results(df, y_pred, run_id, output_file)

#     upload_file_to_s3(output_file,bucket_name,output_file)

#     return output_file

@flow(log_prints=True)
def ride_duration_prediction(
        taxi_type: str,
        run_id: str,
        run_date: datetime = None):
    
    if run_date is None:
        ctx = get_run_context()
        run_date = ctx.flow_run.expected_start_time
    
    input_file, output_file = get_paths(run_date, taxi_type, run_id)

    logger = get_run_logger()

    logger.info(f'reading the data from {input_file}...')
    df = preprocess_dataframe(input_file)
    dicts = prepare_dictionaries(df)

    logger.info(f'loading the model with RUN_ID={run_id}...')
    model = load_model(run_id)

    logger.info(f'applying the model...')
    y_pred = make_prediction(model,dicts)

    logger.info(f'saving the result to {output_file}...')

    save_results(df, y_pred, run_id, output_file)

    upload_file_to_s3(output_file,bucket_name,output_file)

def run():
    taxi_type = sys.argv[1] # 'green'
    year = int(sys.argv[2]) # 2021
    month = int(sys.argv[3]) # 3
    run_id = sys.argv[4] # 'cc36795ca2fd48e8a176194f450c0ade'

    ride_duration_prediction(
        taxi_type=taxi_type,
        run_id=run_id,
        run_date=datetime(year=year, month=month, day=1)
    )

if __name__ == '__main__':
    run()




