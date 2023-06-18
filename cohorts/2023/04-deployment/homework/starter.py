import pickle
import pandas as pd
import sys

with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def get_path(year,month):
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'


def make_prediction_and_save(df,year,month):
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    df['predicted_duration']=y_pred

    df_output = df[['ride_id','predicted_duration']]
    df_output.to_parquet(
        f"yellow_tripdata_{year:04d}-{month:02d}_duration_prediction.parquet",
        engine='pyarrow',
        compression=None,
        index=False
    )

def run():
    year = int(sys.argv[1]) # 2022
    month = int(sys.argv[2]) # 2
    file_path = get_path(year,month)
    df = read_data(filename=file_path)
    make_prediction_and_save(df,year,month)


if __name__ == '__main__':
    run()