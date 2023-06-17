import os
import pickle
import dotenv
import mlflow
from flask import Flask, request, jsonify
dotenv.load_dotenv(".env")

RUN_ID = os.getenv('RUN_ID')
# RUN_ID = "cc36795ca2fd48e8a176194f450c0ade"
MODEL_S3_PATH = f"s3://s3-mlflow-artifacts-storage/mlflow/12/{RUN_ID}/artifacts/model" 
# logged_model = f's3://mlflow-models-alexey/1/{RUN_ID}/artifacts/model'

# logged_model = f'runs:/{RUN_ID}/model'
model = mlflow.pyfunc.load_model(MODEL_S3_PATH)


def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features


def predict(features):
    preds = model.predict(features)
    return float(preds[0])


app = Flask('duration-prediction')


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    ride = request.get_json()

    features = prepare_features(ride)
    pred = predict(features)

    result = {
        'duration': pred,
        'model_version': RUN_ID
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9696)
