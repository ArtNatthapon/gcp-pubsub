import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import random
from datetime import datetime


def insert_todb_dht11(Date, Time, Temperature, Humidity, Status, Voltage):
    try:
        connection = mysql.connector.connect(host='34.126.103.238',
                                             database='smart_home',
                                             user='NSR_ADMIN',
                                             password='natthapon024299')
            mySql_insert_query = """INSERT INTO DHT11 (Date, Time, Temperature, Humidity, Status, Voltage) \
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