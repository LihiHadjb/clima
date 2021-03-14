import psycopg2
import os
from psycopg2 import Error

def init_db():
    try:

        connection = psycopg2.connect(host="ec2-52-71-161-140.compute-1.amazonaws.com",
                                      dbname="d8a5obfa9du048",
                                      user="nobeehmaarddfb",
                                      password="15956bfd9f88359095c6850be56b5e8ed030d15bdb8449a50834f90fe718a70a")
    except (Exception, Error) as error:
        print("Connecting to PostgreSQL failed!", error)

    cursor = connection.cursor()
    directory = "data"
    for file in os.listdir(directory):
        file_name = os.fsdecode(file)
        with open(file_name, 'r') as f:
            next(f)
            cursor.copy_from(f, 'forecasts',
                             columns=('longitude', 'latitude', 'time', 'temp_celsius', 'precipitation_hr'),
                             sep=',')
    connection.commit()
    print("Table created successfully in PostgreSQL ")

