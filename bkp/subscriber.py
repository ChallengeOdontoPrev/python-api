import os
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

# credential_path = "C:/Users/Kaua/Desktop/Eu/DEV/api-py/test_privateKey.json"
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = "projects/course-bigquery-k/subscriptions/exampleSubscription"


def callback(message):
    print(f"Received message: {message}")
    print(f"Data: {message.data}")
    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}")

with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()