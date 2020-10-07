import mysql.connector
import os, time, random
from mysql.connector import Error
from mysql.connector import errorcode
from google.cloud import pubsub_v1
from mysql.connector import Error, errorcode

column6 = random.randint(3, 5)
############ Configure for Google Cloud #################
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'key.json'
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

def sub_pull():
    '''
       Listening message topic you connect  
    '''
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
                        project_id, sub_name)
    subscriber.subscribe(subscription_path, callback=callback)
    print('Listening for messages on: {}'.format(subscription_path))
    time.sleep(30)

def insert_todb_dht11(Date, Time, Temperature, Humidity, Status, Voltage):
    '''
    1. Function to Connect to Database.
    2. Insert Data have received to database you have connect.
    3. Close the connection when you've complete.
    4. If can't to connect or Insert except will warning your problem.

    '''
    try:
        connection = mysql.connector.connect(host='34.126.103.238',
                                             database='smart_home',
                                             user='NSR_ADMIN',
                                             password='natthapon024299')
        mySql_insert_query = """INSERT INTO DHT11_Sensor (Date, Time, Temperature, Humidity, Status, Voltage) \
                                VALUES (%s,%s,%s,%s,%s,%s)"""
        tupl = (Date, Time, Temperature, Humidity, Status, Voltage)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query, tupl)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into sub table")
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into sub table {}".format(error))

    finally:
        if (connection.is_connected()):
            connection.close()
            print("MySQL connection is closed")

## Main Function
while True:
    sub_pull()
    continue