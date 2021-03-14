import os
import psycopg2

from psycopg2 import Error

LON = "Longitude"
LAT = "Latitude"
FOR_TIME = "forecast_time"
TEMP = "Temperature Celsius"
PERCP = "Precipitation Rate mm/hr"


def login_to_db():
    connection = psycopg2.connect(host="ec2-52-71-161-140.compute-1.amazonaws.com",
                                  dbname="d8a5obfa9du048",
                                  user="nobeehmaarddfb",
                                  password="15956bfd9f88359095c6850be56b5e8ed030d15bdb8449a50834f90fe718a70a")
    return connection


def create_table():
    connection = login_to_db()
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE  IF NOT EXIST forecasts(
        id SERIAL PRIMARY KEY,
        LON real,
        LAT real,
        FOR_TIME timestamp,
        TEMP real,
        PERCP real)
    """)


def load_data():
    connection = login_to_db()
    cursor = connection.cursor()

    # directory = "data"
    # for file in os.listdir(directory):
    #     file_name = os.fsdecode(file)
    #for file_name in ["file1.csv", "file2.csv", "file3.csv"]:
    for file_name in ["file1.csv"]:
        with open(file_name, 'r') as f:
            next(f)
            cursor.copy_from(f, 'forecasts',
                             columns=(LON, LAT, FOR_TIME, TEMP, PERCP),
                             sep=',')
    connection.commit()
