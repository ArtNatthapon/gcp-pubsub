from google.cloud import pubsub_v1
from Subscriber import *
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
project_id = 'pivotal-sprite-285504'
topic_id = 'my-topic'

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project_id, topic_id)

def publish_now(data, name):
    data = str(data)
    message = bytes(data, 'UTF-8')
    future = publisher.publish(topic_path, data=message, origin=name)
    print('Published : ',data)
publish_now(datetime.now(), name='Temp')