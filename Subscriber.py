from google.cloud import pubsub_v1
from mysql.connector import Error, errorcode
from connect import insert_todb_dht11
from datetime import datetime
import os, time, random

column6 = random.randint(3, 5)

############ Configure for Google Cloud #################
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
project_id = 'pivotal-sprite-285504'
sub_name = 'my-sub'
############ Configure for Google Cloud #################

def callback(message):
    print('Received message: {}'.format(message.data))
    #print("Server ",datetime.now())
    
    received = message.data.decode('utf-8')
    if message.attributes:
        print('Attributes:')
        for key in message.attributes:
            value = message.attributes.get(key)
            print('{}: {}'.format(key, value))
            message.ack()
    data = message
    column1 = received[4:12]            # Date
    column2 = received[13:21]           # Time
    column3 = str(received[31:35])      # Temp
    column4 = str(received[37:41])      # Humi
    column5 = str(received[42:44])      # Status
    insert_todb_dht11(Date=column1,Time=column2, Temperature=column3,\
                      Humidity=column4, Status=column5, Voltage=column6)
    '''print('Col1 : ',column1)
    print('Col2 : ',column2)
    print('Col3 : ',column3)
    print("Col4 : ",column4)'''


def sub_pull():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
                        project_id, sub_name)
    subscriber.subscribe(subscription_path, callback=callback)
    print('Listening for messages on: {}'.format(subscription_path))
    time.sleep(30)

while True:
    sub_pull()