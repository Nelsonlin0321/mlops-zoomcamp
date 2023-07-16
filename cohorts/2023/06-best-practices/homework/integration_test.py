#!/usr/bin/env python
# coding: utf-8
import pickle
import pandas as pd
import sys
import os

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL","http://0.0.0.0:4566")

options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
        }
}

def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', OUTPUT_FILE_PATTERN)
    return output_pattern.format(year=year, month=month)


def prepare_data(df,categorical):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df


def main(year,month):

    input_path = get_input_path(year,month)
    output_path = get_output_path(year,month)

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    categorical = ['PULocationID', 'DOLocationID']
    
    df = pd.read_parquet(input_path,storage_options=options)
    df = prepare_data(df,categorical)
    
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())


    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred




    df_result.to_parquet(output_path,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options)


if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    # python batch.py 2023 01
    main(year,month)


# (mlflow) nelsonlin@Nelsons-Mac-mini homework % python integration_test.py 2022 1 
# predicted mean duration: 10.502483457575869