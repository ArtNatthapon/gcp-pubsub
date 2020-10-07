from Publisher import publish_now
from Subscriber import sub_pull, callback
from mysql.connector import Error, errorcode
from datetime import datetime
import mysql.connector, random, time

while True:
    sub_pull()
    continue    