import os
import json
from tabnanny import check
from fastapi import FastAPI
from google.cloud import pubsub_v1, storage, vision
from concurrent.futures import TimeoutError
import firebase_admin
from firebase_admin import credentials,firestore

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "google_key.json"

firebase_admin.initialize_app(credentials.Certificate('serviceAccountCredentials.json'))
db = firestore.client()

def Vision(uri):
    client = vision.ImageAnnotatorClient()

    image = vision.Image()
    image.source.image_uri = uri

    objects = client.object_localization(image=image).localized_object_annotations
    element = []
    for object_ in objects:
        element.append(str.lower(object_.name))
    return element

def launchVision(id):
    data = db.collection('metadata').document(id).get()
    data = data.to_dict()
    uri = data["uri"]
    word = data["word"]
    user = data["userId"]
    return checkWord(uri,word)
    
def checkWord(uri,word):
    result = Vision(uri)
    for i in range(len(result)):
        if result[i] == word:
            return 200
    return 400

subscriber = pubsub_v1.SubscriberClient()

storage_client = storage.Client()
bucket_name ="storage_image_api"
bucket = storage_client.get_bucket(bucket_name)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    id = message.data.decode("utf-8")
    code = launchVision(id)
    if(code == 200):
            print("Trouve")
            db.collection('metadata').document(id).update({"status":"True"})
            message.ack()
            return 200
    print("Pas Trouve")
    db.collection('metadata').document(id).update({"status":"True"})
    message.ack()
    return 400

streaming_pull_future = subscriber.subscribe("projects/third-essence-365119/subscriptions/test-pubsub", callback=callback)
print(f"Listening for messages on projects/third-essence-365119/subscriptions/test-pubsub..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

