from fastapi import FastAPI
from inference_sdk import InferenceHTTPClient

CLIENT = InferenceHTTPClient(
    api_url="https://detect.roboflow.com",
    api_key="ONezQzlkEzxv1bdlGn74"
)

result = CLIENT.infer("1.jpg", model_id="aparelho-dentario/2")

app = FastAPI()


@app.get("/detect")
def root():
    return {
        "result": result
    }
