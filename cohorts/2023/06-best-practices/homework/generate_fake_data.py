from datetime import datetime
import pandas as pd
import os

def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)

data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2), dt(1, 10)),
    (1, 2, dt(2, 2), dt(2, 3)),
    (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),     
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df = pd.DataFrame(data, columns=columns)

INPUT_FILE_PATTERN="s3://nyc-duration/in/2022-01.parquet"

year='2022'
month='01'
input_file=INPUT_FILE_PATTERN

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL","http://0.0.0.0:4566")

options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
        }
}

df.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

# --endpoint-url http://0.0.0.0:4566
# (mlflow) nelsonlin@Nelsons-Mac-mini homework % aws s3 ls s3://nyc-duration/in/ --endpoint-url http://0.0.0.0:4566
# 2023-07-17 00:37:08       3667 2022-01.parquet