import lambda_function

event = {
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


result = lambda_function.lambda_handler(event, None)
print(result)