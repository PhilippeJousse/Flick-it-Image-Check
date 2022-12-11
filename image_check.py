import os, pyrebase,json
from google.cloud import pubsub_v1, storage, vision
from concurrent.futures import TimeoutError

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "google_key.json"

firebaseConfig = {
  "apiKey": "AIzaSyBnz6wws3EjTRnFOG7NvefKSr9CsaOlcxY",
  "authDomain": "flick-it-users-storage.firebaseapp.com",
  "databaseURL": "https://flick-it-users-storage-default-rtdb.europe-west1.firebasedatabase.app",
  "projectId": "flick-it-users-storage",
  "storageBucket": "flick-it-users-storage.appspot.com",
  "messagingSenderId": "1046722019798",
  "appId": "1:1046722019798:web:905b021820e1922f95a477",
  "measurementId": "G-J3T9K8WPV2",
  "serviceAccount": "serviceAccountCredentials.json"
}
firebase = pyrebase.initialize_app(firebaseConfig)

db = firebase.database()

publisher = pubsub_v1.PublisherClient()
topic_path ="projects/third-essence-365119/topics/launch-scoring"

def Vision(uri):
    client = vision.ImageAnnotatorClient()

    image = vision.Image()
    image.source.image_uri = uri

    objects = client.object_localization(image=image).localized_object_annotations
    element = []
    for object_ in objects:
        element.append(str.lower(object_.name))
    return element
    
def checkWord(uri,word):
    result = Vision(uri)
    for i in range(len(result)):
        if result[i] == word:
            return 200
    return 400

def getData(id):
    data = db.child("metadata").child(id).get()
    return json.loads(json.dumps(data.val()))

subscriber = pubsub_v1.SubscriberClient()

storage_client = storage.Client()

def callback(message: pubsub_v1.subscriber.message.Message):
    id = message.data.decode("utf-8")
    message.ack()
    data = getData(id)
    code = checkWord(data["uri"],data["word"])
    if(code == 200):
        print("Trouve")
        db.child("metadata").child(id).update({"status":"True"})
        id = id.encode('utf-8')
        publisher.publish(topic_path,id)
        return 200
    print("Pas Trouve")
    db.child("metadata").child(id).update({"status":"True"})
    return 400

streaming_pull_future = subscriber.subscribe("projects/third-essence-365119/subscriptions/launch-vision-sub", callback=callback)
print(f"Listening for messages on projects/third-essence-365119/subscriptions/launch-vision-sub..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

