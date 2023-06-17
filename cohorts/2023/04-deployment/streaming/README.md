## Machine Learning for Streaming

Reference
* [Tutorial: Using Amazon Lambda with Amazon Kinesis](https://docs.amazonaws.cn/en_us/lambda/latest/dg/with-kinesis-example.html)

### Step 1: Create lambda role

Create the execution role that gives your function permission to access Amazon resources.

Create a role with the following properties.
- Trusted entity – Amazon Lambda.
- Permissions – AWSLambdaKinesisExecutionRole.
- Role name – lambda-kinesis-role.

The AWSLambdaKinesisExecutionRole policy has the permissions that the function needs to read items from Kinesis and write logs to CloudWatch Logs.


### Step 3: Create Lambda  function
Simple python code

```python
import json

def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features

def predict(features):
    return 10

def lambda_handler(event, context):
    
    ride = event[ 'ride']
    ride_id = event['ride_id']
    
    features = prepare_features(ride)
    
    prediction = predict(features)
    
    return {
        'ride duration': prediction,
        'ride_id': ride_id
        
    }

```

### Step 3: Create Kinesis Data Stream as Trigger
```bash
KINESIS_STREAM_INPUT=ride_events
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data "Hello, this is a test."
```

Kinesis event example
```json
{
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49641799221921859026137820326421612608423182380468011010",
                "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                "approximateArrivalTimestamp": 1687005952.873
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49641799221921859026137820326421612608423182380468011010",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::932682266260:role/lambda-kinesis-role",
            "awsRegion": "ap-southeast-1",
            "eventSourceARN": "arn:aws:kinesis:ap-southeast-1:932682266260:stream/ride_events"
        }
    ]
}
```

```json
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data '{
        "ride": {
            "PULocationID": 130,
            "DOLocationID": 205,
            "trip_distance": 3.66
        }, 
        "ride_id": 156
    }'
```

lambda test set
```json
{
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49641799221921859026137820326421612608423182380468011010",
                "data": "eyJyaWRlIjogeyJQVUxvY2F0aW9uSUQiOiAxMzAsICJET0xvY2F0aW9uSUQiOiAyMDUsICJ0cmlw\nX2Rpc3RhbmNlIjogMy42Nn0sICJyaWRlX2lkIjogMTU2fQ==\n",
                "approximateArrivalTimestamp": 1687005952.873
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49641799221921859026137820326421612608423182380468011010",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::932682266260:role/lambda-kinesis-role",
            "awsRegion": "ap-southeast-1",
            "eventSourceARN": "arn:aws:kinesis:ap-southeast-1:932682266260:stream/ride_events"
        }
    ]
}
```

### Step 4: Create another stream for prediction and put records

```python

PREDICTIONS_STREAM_NAME = os.getenv('PREDICTIONS_STREAM_NAME', 'ride_predictions')

prediction_event = {
    'model': 'ride_duration_prediction_model',
    'version': '123',
    'prediction': {
        'ride_duration': prediction,
        'ride_id': ride_id   
    }
}
kinesis_client.put_record(
    StreamName=PREDICTIONS_STREAM_NAME,
    Data=json.dumps(prediction_event),
    PartitionKey=str(ride_id)
)
```

### Step 5: Attach policy to allow lambda to put records to kinesis

```
User: arn:aws:sts::932682266260:assumed-role/lambda-kinesis-role/ride-duration-prediction-test is not authorized to perform: kinesis:PutRecord on resource: arn:aws:kinesis:ap-southeast-1:932682266260:stream/ride_predictions because no identity-based policy allows the kinesis:PutRecord action
```

Create a policy that allows to put records to ARN: arn:aws:kinesis:ap-southeast-1:932682266260:stream/ride_predictions 

<img src=./policy-to-put-records.png width=400><img/>


### Step 6: Get records from ride predictions stream
```sh
KINESIS_STREAM_OUTPUT='ride_predictions'
SHARD='shardId-000000000000'

SHARD_ITERATOR=$(aws kinesis \
    get-shard-iterator \
        --shard-id ${SHARD} \
        --shard-iterator-type TRIM_HORIZON \
        --stream-name ${KINESIS_STREAM_OUTPUT} \
        --query 'ShardIterator' \
)

RESULT=$(aws kinesis get-records --shard-iterator $SHARD_ITERATOR)

echo ${RESULT} | jq -r '.Records[0].Data' | base64 --decode

{"model": "ride_duration_prediction_model", "version": "123", "prediction": {"ride_duration": 10, "ride_id": 156}}    

```

### Step 7: Get model from s3 and predict

```python
RUN_ID = os.getenv('RUN_ID')
logged_model  = f"s3://s3-mlflow-artifacts-storage/mlflow/12/{RUN_ID}/artifacts/model" 

TEST_RUN = os.getenv('TEST_RUN', 'False') == 'True'

model = mlflow.pyfunc.load_model(logged_model)

def predict(features):
    pred = model.predict(features)
    return float(pred[0])
```


### Step 8: Docker file to deploy

```docker
FROM public.ecr.aws/lambda/python:3.9

RUN pip install -U pip
RUN pip install pipenv 

COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN pipenv install --system --deploy

COPY [ "lambda_function.py", "./" ]

CMD [ "lambda_function.lambda_handler" ]

```

### Step 9: Build the docker image

```sh
docker build -t stream-model-duration:v1 . --platform linux/amd64
```

```sh
docker run -it --rm -p 8080:8080 --env-file docker.env stream-model-duration:v1
```

rebuild pipenv env

```
pipenv --rm
rm Pipfile.lock
pipenv install
```


### Step 10: Push ECR

```
aws ecr create-repository --repository-name ny-green-taxi-duration-model
```
```json
{
    "repository": {
        "repositoryArn": "arn:aws:ecr:ap-southeast-1:932682266260:repository/ny-green-taxi-duration-model",
        "registryId": "932682266260",
        "repositoryName": "ny-green-taxi-duration-model",
        "repositoryUri": "932682266260.dkr.ecr.ap-southeast-1.amazonaws.com/ny-green-taxi-duration-model",
        "createdAt": 1687012704.0,
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": false
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}
```

```sh
REMOTE_URL="932682266260.dkr.ecr.ap-southeast-1.amazonaws.com/ny-green-taxi-duration-model"
REMOTE_TAG='v1'
REMOTE_IMAGE=${REMOTE_URL}:${REMOTE_TAG}
LOCAL_IMAGE="stream-model-duration:v1"

docker tag ${LOCAL_IMAGE} ${REMOTE_IMAGE}
docker push ${REMOTE_IMAGE}


aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 932682266260.dkr.ecr.ap-southeast-1.amazonaws.com

```


### Step  11: Create lambda function using ECR Image


### Step 12:
Test
```bash
KINESIS_STREAM_INPUT=ride_events
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data '{
        "ride": {
            "PULocationID": 110,
            "DOLocationID": 25,
            "trip_distance": 100
        }, 
        "ride_id": 20
    }'
```

### Get Prediction from Kinese

```sh
KINESIS_STREAM_OUTPUT='ride_predictions'
SHARD='shardId-000000000000'

SHARD_ITERATOR=$(aws kinesis \
    get-shard-iterator \
        --shard-id ${SHARD} \
        --shard-iterator-type TRIM_HORIZON \
        --stream-name ${KINESIS_STREAM_OUTPUT} \
        --query 'ShardIterator' \
)

RESULT=$(aws kinesis get-records --shard-iterator $SHARD_ITERATOR)

echo ${RESULT} | jq -r '.Records[-1].Data' | base64 --decode
```