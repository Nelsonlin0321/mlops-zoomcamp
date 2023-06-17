import json
import base64
import boto3
import os
import dotenv
import mlflow

dotenv.load_dotenv(".env")

kinesis_client = boto3.client('kinesis')
PREDICTIONS_STREAM_NAME = os.getenv('PREDICTIONS_STREAM_NAME', 'ride_predictions')
RUN_ID = os.getenv('RUN_ID')
logged_model  = f"s3://s3-mlflow-artifacts-storage/mlflow/12/{RUN_ID}/artifacts/model" 

TEST_RUN = os.getenv('TEST_RUN', 'False') == 'True'

model = mlflow.pyfunc.load_model(logged_model)

def get_kinesis_data_records(event):
    data_records = []
    for record in event['Records']:
        data_encoded = record['kinesis']['data']
        data_decoded = base64.b64decode(data_encoded).decode('utf-8')
        data_records.append(data_decoded)
    return data_records 
        
def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features

def predict(features):
    pred = model.predict(features)
    return float(pred[0])

def lambda_handler(event, context):
    
    predictions_events = []

    data_records = get_kinesis_data_records(event)

    for record in data_records:

        record = json.loads(record)
        ride =  record['ride']
        ride_id = record['ride_id']

        features = prepare_features(ride)
        prediction = predict(features)

        prediction_event = {
            "model":"ride_duration_prediction_model",
            "version":'123',
            "prediction":
            {
                'ride_duration':prediction,
                'ride_id':ride_id
            }
        }
        if not TEST_RUN:
            kinesis_client.put_record(
                StreamName=PREDICTIONS_STREAM_NAME,
                Data=json.dumps(prediction_event),
                PartitionKey=str(ride_id)
            )
        
        predictions_events.append(prediction_event)

    # print(json.dumps(predictions))
    

    return {
        'predictions': predictions_events
    }
    