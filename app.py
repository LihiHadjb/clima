from flask import Flask
from psycopg2 import Error
import psycopg2
from psycopg2.extras import RealDictCursor
import json


app = Flask(__name__)

try:

    connection = psycopg2.connect(host="ec2-52-71-161-140.compute-1.amazonaws.com",
                                  dbname="d8a5obfa9du048",
                                  user="nobeehmaarddfb",
                                  password="15956bfd9f88359095c6850be56b5e8ed030d15bdb8449a50834f90fe718a70a")
except (Exception, Error) as error:
    print("Connecting to PostgreSQL failed!", error)

cursor = connection.cursor(cursor_factory=RealDictCursor)


@app.route('/')
def hello_world():
    cursor.execute("SELECT * FROM forecasts WHERE latitude = -90 AND longitude = -179")
    result = cursor.fetchall()
    return result[0]



if __name__ == '__main__':
    app.run()
