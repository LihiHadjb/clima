import os
import psycopg2

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

    # cursor.execute("""CREATE TABLE forecasts(
    #     id SERIAL PRIMARY KEY,
    #     longitude real,
    #     latitude real,
    #     time timestamp,
    #     temp_celsius real,
    #     precipitation_hr real
    # )
    # """)

    # directory = "data"
    # for file in os.listdir(directory):
    #     file_name = os.fsdecode(file)
    #for file_name in ["file1.csv", "file2.csv", "file3.csv"]:
    for file_name in ["file1.csv"]:
        with open(file_name, 'r') as f:
            next(f)
            cursor.copy_from(f, 'forecasts',
                             columns=('longitude', 'latitude', 'time', 'temp_celsius', 'precipitation_hr'),
                             sep=',')
    connection.commit()
    print("Table created successfully in PostgreSQL ")

